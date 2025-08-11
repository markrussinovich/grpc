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

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "src/core/call/metadata.h"
#include "src/core/config/core_configuration.h"
#include "src/core/lib/event_engine/event_engine_context.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/promise/try_seq.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/surface/channel_create.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/server/server.h"
#include "src/core/util/crash.h"
#include "src/core/util/debug_location.h"

// Prepare shared memory backing for the transport pair (datapath WIP).
#include "src/core/ext/transport/shmem/shmem_segment.h"
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
    grpc_shmem::ControlBlock* ctrl_ = nullptr;  // not yet used
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
  const std::shared_ptr<grpc_event_engine::experimental::EventEngine>
      event_engine_;
  const RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  // Shared memory state (datapath under construction)
  std::unique_ptr<grpc_shmem::ShmemSegment> segment_;
  grpc_shmem::ControlBlock* ctrl_ = nullptr;
};

ShmemServerTransport::ShmemServerTransport(const ChannelArgs& args)
    : event_engine_(
          args.GetObjectRef<grpc_event_engine::experimental::EventEngine>()),
      call_arena_allocator_(MakeRefCounted<CallArenaAllocator>(
          args.GetObject<ResourceQuota>()
              ->memory_quota()
              ->CreateMemoryAllocator("shmem_server"),
          1024)) {}

void ShmemServerTransport::SetCallDestination(
    RefCountedPtr<UnstartedCallDestination> unstarted_call_handler) {
  unstarted_call_handler_ = std::move(unstarted_call_handler);
  ConnectionState expect = ConnectionState::kInitial;
  state_.compare_exchange_strong(expect, ConnectionState::kReady,
                                 std::memory_order_acq_rel,
                                 std::memory_order_acquire);
  connected_state()->SetReady();
}

void ShmemServerTransport::Orphan() {
  LOG(INFO) << "ShmemServerTransport::Orphan(): " << this;
  Disconnect(absl::UnavailableError("Server transport closed"));
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
  server_transport_->Disconnect(
      absl::UnavailableError("Client transport closed"));
}

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  child_call_handler.SpawnGuarded(
      "pull_initial_metadata",
      TrySeq(child_call_handler.PullClientInitialMetadata(),
             [server_transport = server_transport_,
              connected_state = server_transport_->connected_state(),
              child_call_handler](ClientMetadataHandle md) mutable {
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
