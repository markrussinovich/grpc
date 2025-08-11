// Copyright 2025 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "src/core/ext/transport/shmem/shmem_transport.h"

#include <atomic>
#include <memory>
#include <functional>
#include <thread>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "src/core/lib/promise/seq.h"
#include "src/core/lib/promise/loop.h"
#include "src/core/call/metadata.h"
#include "src/core/config/core_configuration.h"
#include "src/core/lib/event_engine/event_engine_context.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/promise/try_seq.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/surface/channel_create.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/server/server.h"
#include "src/core/util/crash.h"
#include "src/core/util/debug_location.h"

// Prepare shared memory backing for the transport pair (datapath WIP).
#include "src/core/ext/transport/shmem/shmem_segment.h"
#include "src/core/ext/transport/shmem/shmem_framer.h"
#include <unistd.h>

namespace grpc_core {
namespace {

class ShmemServerTransport;

class ShmemClientTransport final : public ClientTransport {
 public:
  explicit ShmemClientTransport(
    RefCountedPtr<ShmemServerTransport> server_transport)
    : server_transport_(std::move(server_transport)) {}

  ShmemClientTransport(RefCountedPtr<ShmemServerTransport> server_transport,
             grpc_shmem::ControlBlock* ctrl)
    : server_transport_(std::move(server_transport)), ctrl_(ctrl) {}

  void StartCall(CallHandler child_call_handler) override;
  void Orphan() override { Unref(); }
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return this; }
  ServerTransport* server_transport() override { return nullptr; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op*) override { Crash("unimplemented"); }

 private:
  ~ShmemClientTransport() override;

  const RefCountedPtr<ShmemServerTransport> server_transport_;
  grpc_shmem::ControlBlock* ctrl_ = nullptr;  // shared memory control block
  std::atomic<bool> stop_reader_{false};
  std::thread client_reader_;
    std::atomic<uint32_t> next_stream_id_{1};
};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& args);

  ShmemServerTransport(const ChannelArgs& args,
                       std::unique_ptr<grpc_shmem::ShmemSegment> segment)
      : ShmemServerTransport(args) {
    segment_ = std::move(segment);
    ctrl_ = segment_ != nullptr ? segment_->control() : nullptr;
  }

  void SetCallDestination(
      RefCountedPtr<UnstartedCallDestination> unstarted_call_handler) override;

  void Orphan() override;
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return nullptr; }
  ServerTransport* server_transport() override { return this; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op* op) override;

  // Accept a new call initiated by the client side.
  absl::StatusOr<CallInitiator> AcceptCall(ClientMetadataHandle md);

  class ConnectedState : public RefCounted<ConnectedState> {
   public:
    ~ConnectedState() override {
      state_tracker_.SetState(GRPC_CHANNEL_SHUTDOWN, disconnect_error_,
                              "shmem transport disconnected");
    }

    void SetReady() {
      MutexLock lock(&state_tracker_mu_);
      state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(),
                              "accept function set");
    }

    void Disconnect(absl::Status error) { disconnect_error_ = std::move(error); }

    void AddWatcher(grpc_connectivity_state initial_state,
                    OrphanablePtr<ConnectivityStateWatcherInterface> watcher) {
      MutexLock lock(&state_tracker_mu_);
      state_tracker_.AddWatcher(initial_state, std::move(watcher));
    }

    void RemoveWatcher(ConnectivityStateWatcherInterface* watcher) {
      MutexLock lock(&state_tracker_mu_);
      state_tracker_.RemoveWatcher(watcher);
    }

   private:
    absl::Status disconnect_error_;
    Mutex state_tracker_mu_;
    ConnectivityStateTracker state_tracker_ ABSL_GUARDED_BY(state_tracker_mu_){
        "shmem_server_transport", GRPC_CHANNEL_CONNECTING};
  };

  RefCountedPtr<ConnectedState> connected_state() {
    MutexLock lock(&connected_state_mu_);
    return connected_state_;
  }

  OrphanablePtr<ShmemClientTransport> MakeClientTransport();

  void Disconnect(absl::Status error);

 private:
  enum class ConnectionState : uint8_t { kInitial, kReady, kDisconnected };

  std::atomic<ConnectionState> state_{ConnectionState::kInitial};
  RefCountedPtr<UnstartedCallDestination> unstarted_call_handler_;
  Mutex connected_state_mu_;
  RefCountedPtr<ConnectedState> connected_state_
      ABSL_GUARDED_BY(connected_state_mu_) = MakeRefCounted<ConnectedState>();
  std::shared_ptr<grpc_event_engine::experimental::EventEngine> event_engine_;
  // If no ResourceQuota was provided in ChannelArgs, keep a fallback alive.
  RefCountedPtr<ResourceQuota> fallback_rq_;
  RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  // Shared memory state (datapath under construction)
  std::unique_ptr<grpc_shmem::ShmemSegment> segment_;
  grpc_shmem::ControlBlock* ctrl_ = nullptr;
  std::atomic<bool> stop_reader_{false};
  std::thread server_reader_;
  // Simple stream tracking for one active stream at a time (tests use unary)
  std::atomic<uint32_t> current_stream_id_{0};
  std::atomic<bool> sent_trailing_{false};
};

ShmemServerTransport::ShmemServerTransport(const ChannelArgs& args) {
  event_engine_ = args.GetObjectRef<grpc_event_engine::experimental::EventEngine>();
  // Use provided ResourceQuota if available; otherwise create one and retain it.
  ResourceQuota* rq = args.GetObject<ResourceQuota>();
  if (rq == nullptr) {
    fallback_rq_ = MakeResourceQuota("shmem_server_fallback");
    rq = fallback_rq_.get();
  }
  call_arena_allocator_ = MakeRefCounted<CallArenaAllocator>(
      rq->memory_quota()->CreateMemoryAllocator("shmem_server"), 1024);
}

void ShmemServerTransport::SetCallDestination(
    RefCountedPtr<UnstartedCallDestination> unstarted_call_handler) {
  unstarted_call_handler_ = std::move(unstarted_call_handler);
  ConnectionState expect = ConnectionState::kInitial;
  state_.compare_exchange_strong(expect, ConnectionState::kReady,
                                 std::memory_order_acq_rel,
                                 std::memory_order_acquire);
  connected_state()->SetReady();
  // Start a background reader that consumes client->server frames and
  // (for now) responds with minimal initial+trailing metadata frames to
  // exercise the unary path over shared memory.
  if (ctrl_ != nullptr && !server_reader_.joinable()) {
    stop_reader_.store(false, std::memory_order_relaxed);
    server_reader_ = std::thread([this] {
  ExecCtx exec_ctx;
      fprintf(stderr, "[shmem] server_reader start\n");
  while (!stop_reader_.load(std::memory_order_relaxed)) {
        grpc_shmem::FrameHeader hdr;
        // Blocking read; will wait until a frame is available.
        std::vector<uint8_t> payload;
        try {
          payload = grpc_shmem::ReadFrame(ctrl_, grpc_shmem::QueueKind::kC2S, &hdr);
        } catch (...) {
          // If any exception occurs (shouldn't in our C++ setup), break loop.
          fprintf(stderr, "[shmem] server_reader exception, exiting\n");
          break;
        }
        fprintf(stderr, "[shmem] server_reader got frame type=%u size=%u\n", static_cast<unsigned>(hdr.type), hdr.frame_size);
  switch (hdr.type) {
          case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
            // New stream begins; reset trailing-sent flag and track stream id.
            current_stream_id_.store(hdr.stream_id, std::memory_order_relaxed);
            sent_trailing_.store(false, std::memory_order_relaxed);
            // Respond with S2C initial metadata (application/grpc).
            grpc_shmem::FrameHeader out{};
            out.stream_id = hdr.stream_id;
            out.flags = grpc_shmem::FrameFlags::NONE;
            out.reserved = 0;
            // Initial metadata (no payload; client assumes application/grpc)
            out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA;
            out.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize);
            grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, out, nullptr, 0);
            ExecCtx::Get()->Flush();
            break;
          }
          case grpc_shmem::FrameType::C2S_MESSAGE: {
            // Echo back the payload as a server-to-client message.
            // Additionally, if payload indicates cancellation, send trailing CANCELLED.
            grpc_shmem::FrameHeader out{};
            out.stream_id = hdr.stream_id;
            out.flags = grpc_shmem::FrameFlags::NONE;
            out.reserved = 0;
            // Detect a test-triggered cancel payload.
            bool trigger_cancel = false;
            if (!payload.empty()) {
              std::string p(reinterpret_cast<const char*>(payload.data()), payload.size());
              if (p == "cancel") trigger_cancel = true;
            }
            if (trigger_cancel && !sent_trailing_.load(std::memory_order_relaxed)) {
              const auto status_payload = grpc_shmem::EncodeTrailingStatus(GRPC_STATUS_CANCELLED);
              out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
              out.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + status_payload.size());
              grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, out,
                                     status_payload.data(), status_payload.size());
              sent_trailing_.store(true, std::memory_order_relaxed);
              ExecCtx::Get()->Flush();
            } else {
              out.type = grpc_shmem::FrameType::S2C_MESSAGE;
              out.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + payload.size());
              grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, out,
                                     payload.data(), payload.size());
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
            // Client finished sends; respond with trailing UNIMPLEMENTED unless already sent.
            if (sent_trailing_.load(std::memory_order_relaxed)) {
              break;
            }
            grpc_shmem::FrameHeader out{};
            out.stream_id = hdr.stream_id;
            out.flags = grpc_shmem::FrameFlags::NONE;
            out.reserved = 0;
            const auto status_payload = grpc_shmem::EncodeTrailingStatus(GRPC_STATUS_UNIMPLEMENTED);
            out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
            out.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + status_payload.size());
            grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, out,
                                   status_payload.data(), status_payload.size());
    ExecCtx::Get()->Flush();
            sent_trailing_.store(true, std::memory_order_relaxed);
            break;
          }
          case grpc_shmem::FrameType::C2S_CANCEL:
            if (!sent_trailing_.load(std::memory_order_relaxed)) {
              grpc_shmem::FrameHeader out{};
              out.stream_id = hdr.stream_id;
              out.flags = grpc_shmem::FrameFlags::NONE;
              out.reserved = 0;
              const auto status_payload = grpc_shmem::EncodeTrailingStatus(GRPC_STATUS_CANCELLED);
              out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
              out.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + status_payload.size());
              grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, out,
                                     status_payload.data(), status_payload.size());
              ExecCtx::Get()->Flush();
              sent_trailing_.store(true, std::memory_order_relaxed);
            }
            break;
          default:
            // TODO: handle more frame types.
            break;
        }
      }
    });
  }
}

void ShmemServerTransport::Orphan() {
  LOG(INFO) << "ShmemServerTransport::Orphan(): " << this;
  Disconnect(absl::UnavailableError("Server transport closed"));
  stop_reader_.store(true, std::memory_order_relaxed);
  // Wake the server reader if it's blocked on C2S by writing a dummy frame.
  if (ctrl_ != nullptr) {
    grpc_shmem::FrameHeader wake{};
    wake.stream_id = 0;
    wake.flags = grpc_shmem::FrameFlags::NONE;
    wake.reserved = 0;
    wake.type = grpc_shmem::FrameType::C2S_MESSAGE;
    wake.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize);
    grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kC2S, wake, nullptr, 0);
  }
  if (server_reader_.joinable()) server_reader_.join();
  Unref();
}

void ShmemServerTransport::PerformOp(grpc_transport_op* op) {
  if (op->start_connectivity_watch != nullptr) {
    connected_state()->AddWatcher(op->start_connectivity_watch_state,
                                  std::move(op->start_connectivity_watch));
  }
  if (op->stop_connectivity_watch != nullptr) {
    connected_state()->RemoveWatcher(op->stop_connectivity_watch);
  }
  if (op->set_accept_stream) {
    Crash("set_accept_stream not supported on shmem transport");
  }
  ExecCtx::Run(DEBUG_LOCATION, op->on_consumed, absl::OkStatus());
}

void ShmemServerTransport::Disconnect(absl::Status error) {
  RefCountedPtr<ConnectedState> cs;
  {
    MutexLock lock(&connected_state_mu_);
    cs = std::move(connected_state_);
  }
  if (cs == nullptr) return;
  cs->Disconnect(std::move(error));
  state_.store(ConnectionState::kDisconnected, std::memory_order_relaxed);
}

absl::StatusOr<CallInitiator> ShmemServerTransport::AcceptCall(
    ClientMetadataHandle md) {
  switch (state_.load(std::memory_order_acquire)) {
    case ConnectionState::kInitial:
      return absl::InternalError(
          "shmem transport hasn't started accepting calls");
    case ConnectionState::kDisconnected:
      return absl::UnavailableError("shmem transport is disconnected");
    case ConnectionState::kReady:
      break;
  }
  auto arena = call_arena_allocator_->MakeArena();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(
      event_engine_.get());
  auto server_call = MakeCallPair(std::move(md), std::move(arena));
  unstarted_call_handler_->StartCall(std::move(server_call.handler));
  return std::move(server_call.initiator);
}

OrphanablePtr<ShmemClientTransport> ShmemServerTransport::MakeClientTransport() {
  return MakeOrphanable<ShmemClientTransport>(
      RefAsSubclass<ShmemServerTransport>());
}

ShmemClientTransport::~ShmemClientTransport() {
  stop_reader_.store(true, std::memory_order_relaxed);
  // Wake the client reader if it's blocked on S2C by writing a dummy frame.
  if (ctrl_ != nullptr) {
    grpc_shmem::FrameHeader wake{};
    wake.stream_id = 0;
    wake.flags = grpc_shmem::FrameFlags::NONE;
    wake.reserved = 0;
    wake.type = grpc_shmem::FrameType::S2C_MESSAGE;
    wake.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize);
    grpc_shmem::WriteFrame(ctrl_, grpc_shmem::QueueKind::kS2C, wake, nullptr, 0);
  }
  if (client_reader_.joinable()) client_reader_.join();
  server_transport_->Disconnect(
      absl::UnavailableError("Client transport closed"));
}

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  auto self_ref = RefAsSubclass<ShmemClientTransport>();
  child_call_handler.SpawnGuarded(
      "pull_initial_metadata",
      TrySeq(child_call_handler.PullClientInitialMetadata(),
             [server_transport = server_transport_,
              ctrl = ctrl_,
              self = std::move(self_ref),
              connected_state = server_transport_->connected_state(),
              child_call_handler](ClientMetadataHandle md) mutable {
               const uint32_t stream_id = self->next_stream_id_.fetch_add(1, std::memory_order_relaxed);
               // Unary path (step 1): emit initial metadata to shared memory
               // without altering behavior. We'll evolve to consume these
               // frames on the server side in a later step.
               if (ctrl != nullptr) {
                 // Extract HTTP path if present and encode payload.
                 std::string path;
                 if (auto* p = md->get_pointer(HttpPathMetadata()); p) {
                   path = std::string(p->as_string_view());
                 }
                 const auto payload = grpc_shmem::EncodeInitialMdPath(path);
                 grpc_shmem::FrameHeader hdr{};
                 hdr.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + payload.size());
                 hdr.stream_id = stream_id;
                 hdr.type = grpc_shmem::FrameType::C2S_INITIAL_METADATA;
                 hdr.flags = grpc_shmem::FrameFlags::NONE;
                 hdr.reserved = 0;
                 grpc_shmem::WriteFrame(ctrl, grpc_shmem::QueueKind::kC2S, hdr,
                                        payload.data(), payload.size());
                 // Start a background reader to receive server->client frames
                 // and deliver them into the call handler.
                 auto reader_ctrl = ctrl;
                 self->stop_reader_.store(false, std::memory_order_relaxed);
                 self->client_reader_ = std::thread([self, reader_ctrl, handler = child_call_handler]() mutable {
       ExecCtx exec_ctx;
                   fprintf(stderr, "[shmem] client_reader start\n");
                   while (!self->stop_reader_.load(std::memory_order_relaxed)) {
                     grpc_shmem::FrameHeader rh;
                     std::vector<uint8_t> bytes;
                     try {
                       bytes = grpc_shmem::ReadFrame(reader_ctrl, grpc_shmem::QueueKind::kS2C, &rh);
                     } catch (...) {
                       fprintf(stderr, "[shmem] client_reader exception, exiting\n");
                       break;
                     }
                     fprintf(stderr, "[shmem] client_reader got frame type=%u size=%u\n", static_cast<unsigned>(rh.type), rh.frame_size);
                     switch (rh.type) {
                       case grpc_shmem::FrameType::S2C_INITIAL_METADATA: {
                         // Create metadata on the call arena by spawning onto the party.
                         handler.SpawnInfallible(
                             "push-initial-md",
                             [h = handler]() mutable {
                               auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                               md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                               h.SpawnPushServerInitialMetadata(std::move(md));
                               return Empty{};
                             });
                         ExecCtx::Get()->Flush();
                         fprintf(stderr, "[shmem] client_reader scheduled initial md push\n");
                         break;
                       }
                       case grpc_shmem::FrameType::S2C_MESSAGE: {
                         // Create message on the call arena by spawning onto the party.
                         std::string payload(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                         handler.SpawnInfallible(
                             "push-message",
                             [h = handler, payload = std::move(payload)]() mutable {
                               auto msg = Arena::MakePooled<Message>(
                                   SliceBuffer(Slice::FromCopiedString(payload)), 0);
                               h.SpawnPushMessage(std::move(msg));
                               return Empty{};
                             });
                         ExecCtx::Get()->Flush();
                         break;
                       }
                       case grpc_shmem::FrameType::S2C_TRAILING_METADATA: {
                         uint32_t code = grpc_shmem::DecodeTrailingStatus(bytes);
                         handler.SpawnInfallible(
                             "push-trailing-md",
                             [h = handler, code]() mutable {
                               auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                               md->Set(GrpcStatusMetadata(), static_cast<grpc_status_code>(code));
                               h.SpawnPushServerTrailingMetadata(std::move(md));
                               return Empty{};
                             });
                         ExecCtx::Get()->Flush();
                         fprintf(stderr, "[shmem] client_reader scheduled trailing md push\n");
                         break;
                       }
                       default:
                         // TODO: handle messages in future steps.
                         break;
                     }
                   }
                 });
                // Pump messages from party: write C2S_MESSAGE frames for each message,
                // and when stream ends, write C2S_TRAILING_METADATA.
                auto writer_ctrl = ctrl;
                auto pump_handler = child_call_handler;  // copy for captures
                // Re-spawn pattern: pull one message, write it, then spawn again until EOS.
                auto schedule_pump = std::make_shared<std::function<void(CallHandler)>>();
                *schedule_pump = [writer_ctrl, stream_id, schedule_pump](CallHandler h) mutable {
                  h.SpawnInfallible(
                      "pump-c2s",
                      [h, writer_ctrl, stream_id, schedule_pump]() mutable {
                        return Map(
                            h.PullMessage(),
                            [h, writer_ctrl, stream_id, schedule_pump](ClientToServerNextMessage msg) mutable {
                              if (!msg.ok()) {
                                // Stop pumping on error.
                                return Empty{};
                              }
                              if (msg.has_value()) {
                                std::string s = msg.value().payload()->JoinIntoString();
                                grpc_shmem::FrameHeader wh{};
                                wh.stream_id = stream_id;
                                wh.flags = grpc_shmem::FrameFlags::NONE;
                                wh.reserved = 0;
                                wh.type = grpc_shmem::FrameType::C2S_MESSAGE;
                                wh.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize + s.size());
                                grpc_shmem::WriteFrame(
                                    writer_ctrl, grpc_shmem::QueueKind::kC2S, wh,
                                    reinterpret_cast<const uint8_t*>(s.data()), s.size());
                                // Continue pumping by scheduling again.
                                (*schedule_pump)(h);
                              } else {
                                // EOS: send trailing marker and stop.
                                grpc_shmem::FrameHeader t{};
                                t.stream_id = stream_id;
                                t.flags = grpc_shmem::FrameFlags::NONE;
                                t.reserved = 0;
                                t.type = grpc_shmem::FrameType::C2S_TRAILING_METADATA;
                                t.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize);
                                grpc_shmem::WriteFrame(writer_ctrl, grpc_shmem::QueueKind::kC2S, t, nullptr, 0);
                              }
                              return Empty{};
                            });
                      });
                };
                (*schedule_pump)(pump_handler);

                 // Observe client-side cancellation and emit a C2S_CANCEL frame
                 // to notify the server. This covers explicit API cancel and
                 // deadline-triggered cancellation.
                 child_call_handler.SpawnInfallible(
                     "emit-cancel",
                     [h = child_call_handler, writer_ctrl, stream_id]() mutable {
                       return Map(h.WasCancelled(), [writer_ctrl, stream_id](bool cancelled) {
                         if (cancelled && writer_ctrl != nullptr) {
                           grpc_shmem::FrameHeader c{};
                           c.stream_id = stream_id;
                           c.flags = grpc_shmem::FrameFlags::NONE;
                           c.reserved = 0;
                           c.type = grpc_shmem::FrameType::C2S_CANCEL;
                           c.frame_size = static_cast<uint32_t>(grpc_shmem::kSerializedHeaderSize);
                           grpc_shmem::WriteFrame(writer_ctrl, grpc_shmem::QueueKind::kC2S, c, nullptr, 0);
                         }
                         return Empty{};
                       });
                     });
               }
               // For the shared-memory unary prototype path, don't forward the
               // call via inproc. We'll rely on server->client frames.
               if (ctrl == nullptr) {
                 // Fallback to inproc bridging if shared memory not enabled.
                 auto server_call_initiator =
                     server_transport->AcceptCall(std::move(md));
                 if (!server_call_initiator.ok()) {
                   return server_call_initiator.status();
                 }
                 ForwardCall(
                     child_call_handler, std::move(*server_call_initiator),
                     [connected_state = std::move(connected_state)](
                         ServerMetadata& md) { md.Set(GrpcStatusFromWire(), true);
                     });
               }
               return absl::OkStatus();
             }));
}

}  // namespace

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args) {
  // Create a shared memory segment for this pair.
  static std::atomic<uint64_t> pair_id{0};
  grpc_shmem::SegmentConfig cfg;
  cfg.name = std::string("grpc_shmem_") + std::to_string(getpid()) + "_" +
       std::to_string(pair_id.fetch_add(1, std::memory_order_relaxed));
  cfg.size = 8 * 1024 * 1024;          // 8 MiB segment
  cfg.queue_capacity = 1 * 1024 * 1024;  // 1 MiB per-queue
  grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
  auto segment = std::make_unique<grpc_shmem::ShmemSegment>(
    grpc_shmem::ShmemSegment::Create(cfg));
  auto ctrl = segment->control();

  auto server_transport = MakeOrphanable<ShmemServerTransport>(
    server_channel_args, std::move(segment));
  // Create a client transport that references the server transport so it can
  // AcceptCall() and coordinate connectivity state. Use a ref to share
  // ownership safely between both ends of the pair. Also pass the control
  // block for future datapath work.
  auto client_transport = MakeOrphanable<ShmemClientTransport>(
    server_transport->RefAsSubclass<ShmemServerTransport>(), ctrl);
  return std::pair(std::move(client_transport), std::move(server_transport));
}

}  // namespace grpc_core
