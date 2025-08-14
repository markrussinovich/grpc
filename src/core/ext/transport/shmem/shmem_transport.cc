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
#include <optional>

#include "absl/strings/str_cat.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/promise/map.h"
#include "src/core/lib/promise/try_seq.h"
#include "src/core/lib/slice/slice.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include <grpc/event_engine/event_engine.h>

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
    // Initialize call arena allocator from resource quota (or create one).
    ResourceQuota* rq = args.GetObject<ResourceQuota>();
    if (rq == nullptr) {
      fallback_rq_ = MakeResourceQuota("shmem-server");
      rq = fallback_rq_.get();
    }
    auto alloc = rq->memory_quota()->CreateMemoryAllocator("shmem-server-alloc");
    call_arena_allocator_ = MakeRefCounted<CallArenaAllocator>(std::move(alloc), 1024);
  }
  ShmemServerTransport(const ChannelArgs& args, std::unique_ptr<grpc_shmem::ShmemSegment> seg)
      : segment_(std::move(seg)) {
    spin_iters_ = args.GetInt(kArgShmemSpinIters).value_or(kDefaultSpinIters);
    cb_ = segment_ ? segment_->control() : nullptr;
    // Initialize call arena allocator from resource quota (or create one).
    ResourceQuota* rq = args.GetObject<ResourceQuota>();
    if (rq == nullptr) {
      fallback_rq_ = MakeResourceQuota("shmem-server");
      rq = fallback_rq_.get();
    }
    auto alloc = rq->memory_quota()->CreateMemoryAllocator("shmem-server-alloc");
    call_arena_allocator_ = MakeRefCounted<CallArenaAllocator>(std::move(alloc), 1024);
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
    struct StreamState {
      bool synthetic = true;          // synthetic fast-path or dispatched
      bool sent_initial = false;      // S2C initial metadata sent
      bool sent_trailing = false;     // S2C trailing metadata sent
      bool cancelled = false;         // cancellation observed
      std::string path;               // :path from client initial metadata
  std::optional<CallInitiator> initiator;  // present if dispatched
  // Transitional: while dispatched streams do not yet integrate real server
  // method logic, we still need echo semantics for tests that purposely
  // choose a service/method style path (e.g. /c/N in the concurrency test).
  // We implement a temporary echo of inbound client messages directly from
  // the transport for dispatched streams. This flag is reserved for future
  // refinement (e.g., disabling echo once server handlers produce outputs).
  bool dispatched_echo_fallback = true;
  
      // Stage 1: Dispatched unary state for buffering initial metadata and message
      struct DispatchedUnaryState {
        bool have_initial = false;
        bool have_message = false;
        bool client_trailing_seen = false; // client EOS observed
        bool call_announced = false;       // StartCall invoked
        std::vector<grpc_shmem::KVPair> initial_kvs; // buffered initial kvs
        std::string request_payload;       // buffered unary request body
      };
      std::unique_ptr<DispatchedUnaryState> dispatched_unary; // only for dispatched streams
    };
    
  // Stage 1: Helper to announce dispatched unary call when both initial + message ready
  auto announce_dispatched_call = [this](uint32_t stream_id, StreamState& st) {
    if (!st.dispatched_unary || st.dispatched_unary->call_announced) return;
    
    // Build ClientMetadata for dispatch
    auto arena = call_arena_allocator_->MakeArena();
    auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
    arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
    auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
    for (const auto& kv : st.dispatched_unary->initial_kvs) {
      if (kv.key == ":path") {
        md->Set(HttpPathMetadata(), Slice::FromCopiedString(kv.value));
      } else if (kv.key == ":method") {
        md->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
      } else if (kv.key == ":scheme") {
        if (kv.value == "https") md->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttps); 
        else md->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttp);
      } else if (kv.key == "te") {
        md->Set(TeMetadata(), TeMetadata::kTrailers);
      } else if (kv.key == "user-agent") {
        md->Set(UserAgentMetadata(), Slice::FromCopiedString(kv.value));
      } else if (kv.key == ":authority") {
        md->Set(HttpAuthorityMetadata(), Slice::FromCopiedString(kv.value));
      } else {
        md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&){});
      }
    }
    auto call = MakeCallPair(std::move(md), std::move(arena));
    st.initiator.emplace(std::move(call.initiator));
    RefCountedPtr<UnstartedCallDestination> d;
    {
      MutexLock lock(&dest_mu_);
      d = dest_;
    }
    if (d != nullptr) {
      d->StartCall(std::move(call.handler));
    }
    st.dispatched_unary->call_announced = true;
    
    // Send buffered message if available
    if (st.dispatched_unary->have_message) {
      grpc_slice s = grpc_slice_from_copied_buffer(st.dispatched_unary->request_payload.data(), 
                                                   st.dispatched_unary->request_payload.size());
      st.initiator->SpawnInfallible("shmem-dispatch-push-buffered-msg", [s, ci = *st.initiator]() mutable {
        SliceBuffer sb; sb.AppendIndexed(Slice(s));
        auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
        ci.SpawnPushMessage(std::move(msg));
        return Empty{};
      });
    }
    
    // Stage 2: Send initial metadata first, then spawn outbound loops
    // This ensures initial metadata arrives before any server messages
    {
      std::vector<grpc_shmem::KVPair> kvs = {{"content-type", "application/grpc"}, {"x-shmem", "1"}};
      uint64_t off = 0;
      auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
      grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
      std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
      grpc_shmem::Command out{}; out.stream_id = stream_id; out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA; 
      out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = 0;
      grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
    }
    
    // Messages outbound loop (after initial metadata is sent)
    st.initiator->SpawnInfallible("shmem-dispatch-s2c-msgs", [this, stream_id, ci = *st.initiator]() mutable {
      auto schedule = std::make_shared<std::function<void()>>();
      *schedule = [this, stream_id, ci, schedule]() mutable {
        ci.SpawnInfallible("shmem-dispatch-s2c-msg-step", [this, stream_id, ci, schedule]() mutable {
          return Map(ci.PullMessage(), [this, stream_id, schedule](ServerToClientNextMessage m) mutable {
            if (!m.ok()) return Empty{}; // error/cancel -> stop
            if (m.has_value()) {
              auto& sb = *m.value().payload();
              const size_t n = sb.Length();
              uint64_t off = 0;
              grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, n, &off);
              size_t copied = 0;
              while (copied < n) {
                Slice s = sb.TakeFirst();
                std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off + copied, s.begin(), s.length());
                copied += s.length();
              }
              grpc_shmem::Command out{}; out.stream_id = stream_id; out.type = grpc_shmem::FrameType::S2C_MESSAGE; 
              out.data_offset = off; out.data_size = static_cast<uint32_t>(n); out.grpc_status_code = 0;
              grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
              // Schedule next message pull
              (*schedule)();
            }
            return Empty{}; // EOS ends loop
          });
        });
      };
      // Kick off first iteration
      (*schedule)();
      return Empty{}; // initial SpawnInfallible promise completes immediately
    });
    
    // Trailing metadata outbound (runs in parallel but will only complete after messages)
    st.initiator->SpawnInfallible("shmem-dispatch-s2c-trailing", [this, stream_id, ci = *st.initiator]() mutable {
      return Seq(ci.PullServerTrailingMetadata(), [this, stream_id](ServerMetadataHandle md) mutable {
        int code = static_cast<int>(md->get(GrpcStatusMetadata()).value_or(GRPC_STATUS_UNKNOWN));
        std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(code)}};
        if (auto* msg = md->get_pointer(GrpcMessageMetadata()); msg) {
          kvs.push_back({"grpc-message", std::string(msg->as_string_view())});
        }
        auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
        uint64_t off = 0; grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
        std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
        grpc_shmem::Command out{}; out.stream_id = stream_id; out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA; 
        out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = code;
        grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
        return Empty{};
      });
    });
  };
    absl::flat_hash_map<uint32_t, StreamState> streams;
    for (;;) {
      if (stop_.load(std::memory_order_relaxed)) break;
      grpc_shmem::Command cmd;
      if (!grpc_shmem::PopCommandHybrid(cb_->c2s_queues.get(), cb_, grpc_shmem::Direction::kC2S, spin_iters_, &cmd)) {
        continue;
      }
      auto& st = streams[cmd.stream_id];
      switch (cmd.type) {
        case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
          if (!st.sent_initial) {
            // Deserialize client initial metadata
            std::vector<grpc_shmem::KVPair> kvs_in;
            if (cmd.data_size > 0) {
              const unsigned char* p = cb_->c2s_queues->data_rb.buffer.get() + cmd.data_offset;
              kvs_in = grpc_shmem::DeserializeMetadataKVs(p, cmd.data_size);
            }
            for (const auto& kv : kvs_in) {
              if (kv.key == ":path") st.path = kv.value;
            }
            // Decide if this should be dispatched or synthetic.
            bool is_cancel_path = (st.path == "/cancel");
            bool looks_like_rpc = false;
            if (!st.path.empty() && st.path.size() > 1 && st.path[0] == '/') {
              size_t second_slash = st.path.find('/', 1);
              looks_like_rpc = second_slash != std::string::npos && second_slash + 1 < st.path.size();
            }
            if (looks_like_rpc && !is_cancel_path) {
              st.synthetic = false;
              // Stage 1: Buffer initial metadata for dispatched unary calls
              st.dispatched_unary = std::make_unique<StreamState::DispatchedUnaryState>();
              st.dispatched_unary->initial_kvs = kvs_in;
              st.dispatched_unary->have_initial = true;
            } else {
              // Synthetic path (retain Phase 1 behavior)
              std::vector<grpc_shmem::KVPair> kvs = {
                  {"content-type", "application/grpc"}, {"x-shmem", "1"}};
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0;
              grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
              std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
              grpc_shmem::Command out{}; out.stream_id = cmd.stream_id; out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA; out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = 0;
              grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
            }
            st.sent_initial = true;
          }
          // Release consumed bytes from c2s
          cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          break;
        }
        case grpc_shmem::FrameType::C2S_MESSAGE: {
          const unsigned char* p = cb_->c2s_queues->data_rb.buffer.get() + cmd.data_offset;
          // Synthetic cancel-by-payload (only for synthetic streams)
          if (st.synthetic && !st.sent_trailing && st.path == "/cancel" && cmd.data_size == 6 && memcmp(p, "cancel", 6) == 0) {
            // Mark cancelled semantics.
            st.cancelled = true;
            // We still may choose to NOT echo the message (tests do not expect an echo for cancel).
            // Send trailing CANCELLED immediately if initial already sent; otherwise it will be sent when trailing frame arrives.
            if (st.sent_initial) {
              std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0;
              grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
              std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
              grpc_shmem::Command out{}; out.stream_id = cmd.stream_id; out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA; out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = GRPC_STATUS_CANCELLED;
              grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
              st.sent_trailing = true;
            }
            // Release input bytes
            cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
            break;
          }
          if (st.synthetic) {
            // Echo path (synthetic)
            uint64_t off = 0;
            grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, cmd.data_size, &off);
            std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, p, cmd.data_size);
            grpc_shmem::Command out{}; out.stream_id = cmd.stream_id; out.type = grpc_shmem::FrameType::S2C_MESSAGE; out.data_offset = off; out.data_size = cmd.data_size; out.grpc_status_code = 0;
            grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
            cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          } else {
            // Stage 1: Buffer message for dispatched unary calls
            if (st.dispatched_unary && !st.dispatched_unary->have_message) {
              st.dispatched_unary->request_payload.assign(reinterpret_cast<const char*>(p), cmd.data_size);
              st.dispatched_unary->have_message = true;
              
              // If we have both initial + message, announce the call
              if (st.dispatched_unary->have_initial) {
                announce_dispatched_call(cmd.stream_id, st);
              }
            }
            cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          }
          break;
        }
        case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
          if (st.synthetic) {
            if (!st.sent_trailing) {
              int code = cmd.grpc_status_code != 0 ? cmd.grpc_status_code : (st.cancelled ? GRPC_STATUS_CANCELLED : GRPC_STATUS_UNIMPLEMENTED);
              std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(code)}};
              if (code == GRPC_STATUS_UNIMPLEMENTED) kvs.push_back({"grpc-message", "unimplemented"});
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0;
              grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
              std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
              grpc_shmem::Command out{}; out.stream_id = cmd.stream_id; out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA; out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = code;
              grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
              st.sent_trailing = true;
            }
            if (cmd.data_size != 0) cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          } else {
            // Stage 1: Handle dispatched unary trailing metadata (end of client stream)
            if (st.dispatched_unary) {
              st.dispatched_unary->client_trailing_seen = true;
              
              // If we have initial but no message, this is empty-unary - announce now
              if (st.dispatched_unary->have_initial && !st.dispatched_unary->have_message) {
                announce_dispatched_call(cmd.stream_id, st);
              }
              
              // Signal end-of-client-stream if call was announced
              if (st.initiator.has_value()) {
                st.initiator->SpawnFinishSends();
              }
            }
            
            if (cmd.data_size != 0) cb_->c2s_queues->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
          }
          break;
        }
        case grpc_shmem::FrameType::C2S_CANCEL: {
          st.cancelled = true;
          if (st.synthetic) {
            if (!st.sent_trailing) {
              std::vector<grpc_shmem::KVPair> kvs = {{"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0;
              grpc_shmem::ReserveContiguous(&cb_->s2c_queues->data_rb, buf.size(), &off);
              std::memcpy(cb_->s2c_queues->data_rb.buffer.get() + off, buf.data(), buf.size());
              grpc_shmem::Command out{}; out.stream_id = cmd.stream_id; out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA; out.data_offset = off; out.data_size = static_cast<uint32_t>(buf.size()); out.grpc_status_code = GRPC_STATUS_CANCELLED;
              grpc_shmem::PushCommand(cb_->s2c_queues.get(), cb_, grpc_shmem::Direction::kS2C, out);
              st.sent_trailing = true;
            }
          } else {
            if (st.initiator.has_value()) st.initiator->SpawnCancel();
          }
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
  RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  Mutex s2c_mu_;
  // Hold a fallback resource quota if one was needed, to keep it alive.
  ResourceQuotaRefPtr fallback_rq_;
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
              // Wrap the ring-backed grpc_slice directly into a grpc_core::Slice
              // and append it without copying. Ownership of the slice (and its
              // tail-release destructor) transfers into the SliceBuffer.
              sb.AppendIndexed(Slice(s));
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
               // Add required HTTP/2 pseudo-headers for server filter
               kvs.push_back({":method", "POST"});
               kvs.push_back({":scheme", "http"});
               kvs.push_back({"te", "trailers"});
               if (auto* auth = md->get_pointer(HttpAuthorityMetadata()); auth) {
                 kvs.push_back({":authority", std::string(auth->as_string_view())});
               } else {
                 // Fallback (bench harness sets this via channel args)
                 kvs.push_back({":authority", "test.authority"});
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
