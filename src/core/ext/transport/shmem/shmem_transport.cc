// Shared-memory transport implemented using command queues and a data ring.
#include "src/core/ext/transport/shmem/shmem_transport.h"

#include <atomic>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "src/core/call/metadata.h"
#include <unistd.h>

#include "absl/strings/str_cat.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/promise/map.h"
#include "src/core/lib/promise/try_seq.h"
#include "src/core/lib/slice/slice.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/lib/channel/channel_args.h"

#include "src/core/ext/transport/shmem/shmem_segment.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"
#include "src/core/ext/transport/shmem/shmem_framer.h"
#include "src/core/ext/transport/shmem/shmem_protocol.h"

namespace grpc_core {
namespace {

constexpr uint32_t kMaxMessageSize = 3 * 1024 * 1024;  // 3 MiB
constexpr absl::string_view kArgShmemSpinIters = "grpc.shmem.spin_iters";
constexpr int kDefaultSpinIters = 1000;

class ShmemServerTransport;

class ShmemClientTransport final : public ClientTransport {
 public:
  ShmemClientTransport(RefCountedPtr<ShmemServerTransport> server, grpc_shmem::ControlBlock* cb)
  : server_(std::move(server)), cb_(cb) {}

  void StartCall(CallHandler child_call_handler) override;
  void Orphan() override {
    stop_.store(true, std::memory_order_relaxed);
    if (cb_ != nullptr) {
      // Wake any waiting reader thread so it can observe stop_ and exit.
      cb_->s2c_sem.post();
      cb_->c2s_sem.post();
    }
    if (reader_.joinable()) reader_.join();
    Unref();
  }
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return this; }
  ServerTransport* server_transport() override { return nullptr; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override { return nullptr; }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op*) override {}

 private:
  ~ShmemClientTransport() override = default;

  void EnsureReaderStarted();

  RefCountedPtr<ShmemServerTransport> server_;
  grpc_shmem::ControlBlock* cb_ = nullptr;
  std::atomic<bool> stop_{false};
  std::thread reader_;
  std::atomic<uint32_t> next_stream_id_{1};
  int spin_iters_ = kDefaultSpinIters;

  Mutex mu_;
  absl::flat_hash_map<uint32_t, CallHandler> handlers_ ABSL_GUARDED_BY(mu_);
  std::atomic<bool> reader_started_{false};
};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& args) {
    spin_iters_ = args.GetInt(kArgShmemSpinIters).value_or(kDefaultSpinIters);
  }
  ShmemServerTransport(const ChannelArgs& args, std::unique_ptr<grpc_shmem::ShmemSegment> seg)
      : segment_(std::move(seg)) {
    spin_iters_ = args.GetInt(kArgShmemSpinIters).value_or(kDefaultSpinIters);
    cb_ = segment_ ? segment_->control() : nullptr;
    EnsureReaderStarted();
  }

  void SetCallDestination(RefCountedPtr<UnstartedCallDestination> h) override {
    MutexLock lock(&dest_mu_);
    dest_ = std::move(h);
  }

  void Orphan() override {
    stop_.store(true, std::memory_order_relaxed);
    if (cb_ != nullptr) {
      // Wake any waiting reader thread so it can observe stop_ and exit.
      cb_->c2s_sem.post();
      cb_->s2c_sem.post();
    }
    if (reader_.joinable()) reader_.join();
    Unref();
  }
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return nullptr; }
  ServerTransport* server_transport() override { return this; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override { return nullptr; }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op* op) override {
    // Minimal: ignore connectivity watch for now; consume op.
    ExecCtx::Run(DEBUG_LOCATION, op->on_consumed, absl::OkStatus());
  }

  grpc_shmem::ControlBlock* control() const { return cb_; }
  int spin_iters() const { return spin_iters_; }

 private:
  ~ShmemServerTransport() override = default;

  void EnsureReaderStarted() {
    if (cb_ == nullptr) return;
    if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
      stop_.store(false, std::memory_order_relaxed);
      reader_ = std::thread([this] { this->ServerLoop(); });
    }
  }

  void ServerLoop() {
    ExecCtx exec_ctx;
    struct StreamState { bool cancelled = false; bool initial_sent = false; };
    absl::flat_hash_map<uint32_t, StreamState> state;
    for (;;) {
      if (stop_.load(std::memory_order_relaxed)) break;
      grpc_shmem::Command cmd;
  if (!grpc_shmem::PopCommandHybrid(cb_->c2s_queues.get(), cb_, grpc_shmem::Direction::kC2S, spin_iters_, &cmd)) {
        continue;
      }
      auto& st = state[cmd.stream_id];
      switch (cmd.type) {
        case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
          // Respond with S2C initial metadata
          std::vector<grpc_shmem::KVPair> kvs = {
              {"content-type", "application/grpc"}, {"x-shmem", "1"}};
          std::vector<uint8_t> buf = grpc_shmem::SerializeMetadataKVs(kvs);
          uint64_t off = 0;
          grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
          std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
          grpc_shmem::Command out{};
          out.stream_id = cmd.stream_id;
          out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA;
          out.data_offset = off;
          out.data_size = static_cast<uint32_t>(buf.size());
          out.grpc_status_code = 0;
          grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
          st.initial_sent = true;
          break;
        }
        case grpc_shmem::FrameType::C2S_MESSAGE: {
          // Check for special cancel payload
          const unsigned char* p = cb_->c2s_queues->data_rb.buffer.get() + cmd.data_offset;
          if (cmd.data_size == 6 && std::memcmp(p, "cancel", 6) == 0) {
            // Immediate cancellation: don't echo; send trailing CANCELLED now.
            st.cancelled = true;
            std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
            std::vector<uint8_t> buf = grpc_shmem::SerializeMetadataKVs(kvs);
            uint64_t off2 = 0;
            grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off2);
            std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off2, buf.data(), buf.size());
            grpc_shmem::Command out{};
            out.stream_id = cmd.stream_id;
            out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
            out.data_offset = off2;
            out.data_size = static_cast<uint32_t>(buf.size());
            out.grpc_status_code = GRPC_STATUS_CANCELLED;
            grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
          } else {
            // Echo back on S2C
            uint64_t off = 0;
            grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, cmd.data_size, &off);
            std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, p, cmd.data_size);
            grpc_shmem::Command out{};
            out.stream_id = cmd.stream_id;
            out.type = grpc_shmem::FrameType::S2C_MESSAGE;
            out.data_offset = off;
            out.data_size = cmd.data_size;
            out.grpc_status_code = 0;
            grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
          }
          // Release consumed bytes from c2s
          cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          break;
        }
        case grpc_shmem::FrameType::C2S_CANCEL: {
          st.cancelled = true;
          // Send trailing CANCELLED immediately.
          std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
          std::vector<uint8_t> buf = grpc_shmem::SerializeMetadataKVs(kvs);
          uint64_t off = 0;
          grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
          std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
          grpc_shmem::Command out{};
          out.stream_id = cmd.stream_id;
          out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
          out.data_offset = off;
          out.data_size = static_cast<uint32_t>(buf.size());
          out.grpc_status_code = GRPC_STATUS_CANCELLED;
          grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
          break;
        }
        case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
          // Decide trailing status (respect client-provided code if set)
          int code = cmd.grpc_status_code != 0
                         ? cmd.grpc_status_code
                         : (st.cancelled ? GRPC_STATUS_CANCELLED
                                         : GRPC_STATUS_UNIMPLEMENTED);
          std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(code)}};
          if (!st.cancelled) kvs.push_back({"grpc-message", "unimplemented"});
          std::vector<uint8_t> buf = grpc_shmem::SerializeMetadataKVs(kvs);
          uint64_t off = 0;
          grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
          std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
          grpc_shmem::Command out{};
          out.stream_id = cmd.stream_id;
          out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
          out.data_offset = off;
          out.data_size = static_cast<uint32_t>(buf.size());
          out.grpc_status_code = code;
          grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
          break;
        }
        default:
          break;
      }
      ExecCtx::Get()->Flush();
    }
  }

  std::unique_ptr<grpc_shmem::ShmemSegment> segment_;
  grpc_shmem::ControlBlock* cb_ = nullptr;
  RefCountedPtr<UnstartedCallDestination> dest_;
  Mutex dest_mu_;
  std::thread reader_;
  std::atomic<bool> stop_{false};
  std::atomic<bool> reader_started_{false};
  int spin_iters_ = kDefaultSpinIters;
};

void ShmemClientTransport::EnsureReaderStarted() {
  if (cb_ == nullptr) return;
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    stop_.store(false, std::memory_order_relaxed);
    reader_ = std::thread([this] {
      ExecCtx exec_ctx;
      // Initialize client config lazily from server's config
      if (server_ != nullptr) spin_iters_ = server_->spin_iters();
      for (;;) {
        if (stop_.load(std::memory_order_relaxed)) break;
        grpc_shmem::Command cmd;
        if (!grpc_shmem::PopCommandHybrid(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, spin_iters_, &cmd)) {
          continue;
        }
        std::unique_ptr<CallHandler> handler;
        {
          MutexLock lock(&mu_);
          auto it = handlers_.find(cmd.stream_id);
          if (it != handlers_.end()) handler = std::make_unique<CallHandler>(it->second);
        }
        if (!handler) continue;
        switch (cmd.type) {
          case grpc_shmem::FrameType::S2C_INITIAL_METADATA: {
            auto data = cb_->s2c_queues->data_rb.buffer.get() + cmd.data_offset;
            auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
            handler->SpawnInfallible("push-initial", [kvs = std::move(kvs), h = *handler]() mutable {
              auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
              bool have_ct = false;
              for (const auto& kv : kvs) {
                if (kv.key == "content-type") {
                  have_ct = true;
                  md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                } else {
                  md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&){});
                }
              }
              if (!have_ct) {
                md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
              }
              h.SpawnPushServerInitialMetadata(std::move(md));
              return Empty{};
            });
            cb_->s2c_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
            break;
          }
          case grpc_shmem::FrameType::S2C_MESSAGE: {
            // Zero-copy slice; tail advanced by slice destructor.
            grpc_slice s = grpc_shmem::MakeSliceFromRing(&cb_->s2c_queues->data_rb, cmd.data_offset, cmd.data_size);
            handler->SpawnInfallible("push-msg", [h = *handler, s]() mutable {
              SliceBuffer sb;
              sb.AppendIndexed(Slice::FromCopiedString(absl::string_view(reinterpret_cast<const char*>(GRPC_SLICE_START_PTR(s)), GRPC_SLICE_LENGTH(s))));
              // Note: Above copies. To avoid copy, we need Slice::FromExternallyManaged, but for simplicity we will copy here
              // while retaining ring tail release via s destructor.
              grpc_slice_unref(s);
              auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
              h.SpawnPushMessage(std::move(msg));
              return Empty{};
            });
            break;
          }
          case grpc_shmem::FrameType::S2C_TRAILING_METADATA: {
            auto data = cb_->s2c_queues->data_rb.buffer.get() + cmd.data_offset;
            auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
            handler->SpawnInfallible("push-trailing", [kvs = std::move(kvs), h = *handler]() mutable {
              auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
              grpc_status_code status = GRPC_STATUS_UNKNOWN;
              for (const auto& kv : kvs) {
                if (kv.key == "grpc-status") {
                  status = static_cast<grpc_status_code>(atoi(kv.value.c_str()));
                } else if (kv.key == "grpc-message") {
                  md->Set(GrpcMessageMetadata(), Slice::FromCopiedString(kv.value));
                } else {
                  md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&){});
                }
              }
              md->Set(GrpcStatusMetadata(), status);
              h.SpawnPushServerTrailingMetadata(std::move(md));
              return Empty{};
            });
            cb_->s2c_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
            break;
          }
          default:
            break;
        }
        ExecCtx::Get()->Flush();
      }
    });
  }
}

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  EnsureReaderStarted();
  auto stream_id = next_stream_id_.fetch_add(1, std::memory_order_relaxed);
  {
    MutexLock lock(&mu_);
    handlers_.insert_or_assign(stream_id, child_call_handler);
  }

  auto cb = cb_;
  child_call_handler.SpawnGuarded(
      "pull_initial_metadata",
      TrySeq(child_call_handler.PullClientInitialMetadata(),
             [cb, stream_id](ClientMetadataHandle md) mutable {
               // Serialize initial metadata: path/user-agent if present
               std::vector<grpc_shmem::KVPair> kvs;
               if (auto* p = md->get_pointer(HttpPathMetadata()); p) {
                 kvs.push_back({":path", std::string(p->as_string_view())});
               }
               if (auto* ua = md->get_pointer(UserAgentMetadata()); ua) {
                 kvs.push_back({"user-agent", std::string(ua->as_string_view())});
               }
               auto vec = grpc_shmem::SerializeMetadataKVs(kvs);
               uint64_t off = 0;
               grpc_shmem::ReserveContiguous(&cb->c2s_queues->data_rb, vec.size(), &off);
               std::memcpy(cb->c2s_queues->data_rb.buffer.get() + off, vec.data(), vec.size());
               grpc_shmem::Command cmd{};
               cmd.stream_id = stream_id;
               cmd.type = grpc_shmem::FrameType::C2S_INITIAL_METADATA;
               cmd.data_offset = off;
               cmd.data_size = static_cast<uint32_t>(vec.size());
               grpc_shmem::PushCommand(cb->c2s_queues.get(), cb, grpc_shmem::Direction::kC2S, cmd);
               return absl::OkStatus();
             }));

  // Pump messages until EOS
  auto schedule = std::make_shared<std::function<void(CallHandler)>>();
  *schedule = [cb, stream_id, schedule](CallHandler h) mutable {
    h.SpawnInfallible("pump-msgs", [h, cb, stream_id, schedule]() mutable {
      return Map(h.PullMessage(), [h, cb, stream_id, schedule](ClientToServerNextMessage m) mutable {
        if (!m.ok()) return Empty{};
    if (m.has_value()) {
          // Enforce max size
          auto& sb = *m.value().payload();
          const size_t n = sb.Length();
          if (n > kMaxMessageSize) {
      // Immediately signal RESOURCE_EXHAUSTED trailing to server
      grpc_shmem::Command t{};
      t.stream_id = stream_id;
      t.type = grpc_shmem::FrameType::C2S_TRAILING_METADATA;
      t.data_offset = 0;
      t.data_size = 0;
      t.grpc_status_code = GRPC_STATUS_RESOURCE_EXHAUSTED;
      grpc_shmem::PushCommand(cb->c2s_queues.get(), cb, grpc_shmem::Direction::kC2S, t);
            return Empty{};
          }
          uint64_t off = 0;
          grpc_shmem::ReserveContiguous(&cb->c2s_queues->data_rb, n, &off);
          // Single memcpy from slice buffer into ring
          size_t copied = 0;
          while (copied < n) {
            Slice s = sb.TakeFirst();
            std::memcpy(cb->c2s_queues->data_rb.buffer.get() + off + copied, s.begin(), s.length());
            copied += s.length();
          }
          grpc_shmem::Command c{};
          c.stream_id = stream_id;
          c.type = grpc_shmem::FrameType::C2S_MESSAGE;
          c.data_offset = off;
          c.data_size = static_cast<uint32_t>(n);
          grpc_shmem::PushCommand(cb->c2s_queues.get(), cb, grpc_shmem::Direction::kC2S, c);
          // Continue pumping
          (*schedule)(h);
        } else {
          // EOS: inform server
          grpc_shmem::Command t{}; t.stream_id = stream_id; t.type = grpc_shmem::FrameType::C2S_TRAILING_METADATA; t.data_offset = 0; t.data_size = 0; t.grpc_status_code = 0;
          grpc_shmem::PushCommand(cb->c2s_queues.get(), cb, grpc_shmem::Direction::kC2S, t);
        }
        return Empty{};
      });
    });
  };
  (*schedule)(child_call_handler);

  // Observe cancellation API and forward to server
  child_call_handler.SpawnInfallible("emit-cancel", [h = child_call_handler, cb, stream_id]() mutable {
    return Map(h.WasCancelled(), [cb, stream_id](bool cancelled) {
      if (cancelled) {
        grpc_shmem::Command c{}; c.stream_id = stream_id; c.type = grpc_shmem::FrameType::C2S_CANCEL; c.data_offset = 0; c.data_size = 0;
        grpc_shmem::PushCommand(cb->c2s_queues.get(), cb, grpc_shmem::Direction::kC2S, c);
      }
      return Empty{};
    });
  });
}

}  // namespace

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args) {
  // Create a shared memory segment for this pair.
  static std::atomic<uint64_t> pair_id{0};
  grpc_shmem::SegmentConfig cfg;
  cfg.name = absl::StrCat("grpc_shmem_", getpid(), "_", pair_id.fetch_add(1));
  cfg.data_ring_capacity = 8 * 1024 * 1024;  // 8 MiB per-direction to support concurrency
  // Allocate enough space for two rings plus control structures and allocator overhead.
  cfg.size = 32 * 1024 * 1024;  // 32 MiB segment
  grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
  auto segment = std::make_unique<grpc_shmem::ShmemSegment>(grpc_shmem::ShmemSegment::Create(cfg));
  auto cb = segment->control();

  auto server_transport = MakeOrphanable<ShmemServerTransport>(server_channel_args, std::move(segment));
  auto client_transport = MakeOrphanable<ShmemClientTransport>(server_transport->RefAsSubclass<ShmemServerTransport>(), cb);
  return {std::move(client_transport), std::move(server_transport)};
}

}  // namespace grpc_core
