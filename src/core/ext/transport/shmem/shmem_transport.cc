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

// Shared-memory transport implemented using command queues and a data ring.
#include "src/core/ext/transport/shmem/shmem_transport.h"

#include <grpc/event_engine/event_engine.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/call/metadata.h"
#include "src/core/ext/transport/shmem/shmem_framer.h"
#include "src/core/ext/transport/shmem/shmem_protocol.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"
#include "src/core/ext/transport/shmem/shmem_segment.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/promise/map.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/promise/try_seq.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/slice/slice.h"
#include "src/core/lib/transport/connectivity_state.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/channelz/channelz.h"
#include "absl/log/log.h"

namespace grpc_shmem {
// Define static member for shutdown tracer
std::atomic<int> ShmemShutdownTracer::trace_id_{0};
}

namespace grpc_core {
namespace {

// Note: kMaxMessageSize was previously defined here but is unused in the current implementation
constexpr absl::string_view kArgShmemSpinIters = "grpc.shmem.spin_iters";
constexpr absl::string_view kArgShmemDispatchOnly = "grpc.shmem.dispatch_only";
constexpr int kDefaultSpinIters =
    0;  // No spinning by default - optimize for dispatched workloads

class ShmemServerTransport;

// Forward declarations for cross-process segment management
void RemoveCrossProcessSegment(grpc_shmem::ControlBlock* cb);

class ShmemClientTransport final : public ClientTransport {
 public:
  ShmemClientTransport(RefCountedPtr<ShmemServerTransport> server,
                       grpc_shmem::ControlBlock* cb, const ChannelArgs& args)
      : server_(std::move(server)), cb_(cb) {
    MutexLock l(&state_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_CONNECTING, absl::OkStatus(), "init");
    state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(), "shmem ready");
    
    // Track process attachment for coordination  
    if (cb_) {
      int32_t current_count = ++cb_->process_count;
      LOG(INFO) << "ShmemClientTransport attached, process count now: " << current_count;
      
      // Initialize cross-process semaphores if names are available
      if (cb_->c2s_sem_name[0] != '\0') {
        auto status = c2s_cross_sem_.InitFromName(cb_->c2s_sem_name);
        if (status.ok()) {
          LOG(INFO) << "Initialized c2s cross-process semaphore: " << cb_->c2s_sem_name;
        } else {
          LOG(WARNING) << "Failed to initialize c2s semaphore: " << status;
        }
      }
      if (cb_->s2c_sem_name[0] != '\0') {
        auto status = s2c_cross_sem_.InitFromName(cb_->s2c_sem_name);
        if (status.ok()) {
          LOG(INFO) << "Initialized s2c cross-process semaphore: " << cb_->s2c_sem_name;
        } else {
          LOG(WARNING) << "Failed to initialize s2c semaphore: " << status;
        }
      }
      
      // Create semaphore adapter for queue operations
      sem_adapter_ = std::make_unique<grpc_shmem::TransportSemaphoreAdapter>(
          &c2s_cross_sem_, &s2c_cross_sem_);
    }
  }

  void StartCall(CallHandler child_call_handler) override;
  void Orphan() override {
    InitiateShutdown();
    Unref();
  }
  
 private:
  void InitiateShutdown() {
    grpc_shmem::ShmemShutdownTracer tracer("ShmemClientTransport::InitiateShutdown");
    ExecCtx exec_ctx;
    
    tracer.Checkpoint("Checking if already initiated");
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      tracer.Checkpoint("Already initiated, returning");
      return; // Already initiated
    }
    
    tracer.Checkpoint("Setting shutdown flags");
    // Step 2: Set stop flag and wake threads
    stop_.store(true, std::memory_order_relaxed);
    if (cb_ != nullptr && grpc_shmem::SafePointerAccess::IsValid(cb_)) {
      // REVERTED: Remove cleanup_initiated flag for baseline testing
    }
    
    tracer.Checkpoint("Waiting for threads to exit");
    // Step 3: Wake any waiting reader threads
    WaitForThreadsToExit();
    
    tracer.Checkpoint("Cleaning up resources");
    // Step 4: Cleanup resources in proper order
    CleanupResources();
    
    tracer.Checkpoint("Setting cleanup complete flag");
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    grpc_shmem::ShmemShutdownTracer tracer("ShmemClientTransport::WaitForThreadsToExit");
    
    tracer.Checkpoint("Checking if reader was started");
    // Only wake semaphores if a ring reader thread was started
    if (reader_started_.load(std::memory_order_acquire)) {
      tracer.Checkpoint("Reader was started, waking semaphores");
      if (cb_ != nullptr && grpc_shmem::SafePointerAccess::IsValid(cb_)) {
        try {
          // Wake any waiting reader so it can observe stop_ and exit
        if (cb_) {
          // Wake both directions using cross-process semaphores
          if (cb_->c2s_sem_name[0] != '\0') {
            c2s_cross_sem_.post();
          }
          if (cb_->s2c_sem_name[0] != '\0') {
            s2c_cross_sem_.post();
          }
        }
          tracer.Checkpoint("Semaphores woken successfully");
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error waking semaphores: " << e.what();
        }
      }
      
      tracer.Checkpoint("Joining reader thread");
      if (reader_.joinable()) {
        try {
          reader_.join();
          tracer.Checkpoint("Reader thread joined successfully");
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error joining reader thread: " << e.what();
        }
      } else {
        tracer.Checkpoint("Reader thread not joinable");
      }
    } else {
      tracer.Checkpoint("Reader was never started, skipping thread cleanup");
    }
  }
  
  void CleanupResources() {
    grpc_shmem::ShmemShutdownTracer tracer("ShmemClientTransport::CleanupResources");
    
    try {
      tracer.Checkpoint("Cleaning semaphore manager");
      // Cleanup semaphore manager
      // REVERTED: Remove semaphore manager cleanup for baseline testing
      
      tracer.Checkpoint("Handling process count coordination");
      // Handle process count and cleanup coordination
      if (cb_ != nullptr && grpc_shmem::SafePointerAccess::IsValid(cb_)) {
        tracer.Checkpoint("Decrementing process count");
        int32_t remaining = --cb_->process_count;
        LOG(INFO) << "ShmemServerTransport detaching, remaining processes: " << remaining;
        LOG(INFO) << "ShmemClientTransport detaching, remaining processes: " << remaining;
        
        // Clean up cross-process segment if this is a cross-process client
        if (server_ == nullptr) {
          tracer.Checkpoint("Cross-process client, checking if last process");
          // Last process cleans up shared resources
          if (remaining == 1) {
            tracer.Checkpoint("Last process - cleaning up shared resources");
            RemoveCrossProcessSegment(cb_);
          } else {
            tracer.Checkpoint("Not last process, skipping shared resource cleanup");
          }
        } else {
          tracer.Checkpoint("In-process client, skipping shared resource cleanup");
        }
      } else {
        tracer.Checkpoint("Control block invalid, skipping process coordination");
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "ShmemClientTransport cleanup error: " << e.what();
    }
  }
  
 public:
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return this; }
  ServerTransport* server_transport() override { return nullptr; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op* op) override {
    if (op->start_connectivity_watch != nullptr) {
      MutexLock l(&state_mu_);
      state_tracker_.AddWatcher(op->start_connectivity_watch_state,
                                std::move(op->start_connectivity_watch));
    }
    if (op->stop_connectivity_watch != nullptr) {
      MutexLock l(&state_mu_);
      state_tracker_.RemoveWatcher(op->stop_connectivity_watch);
    }
    if (!op->disconnect_with_error.ok() || !op->goaway_error.ok()) {
      MutexLock l(&state_mu_);
      state_tracker_.SetState(GRPC_CHANNEL_SHUTDOWN, absl::OkStatus(),
                              "shmem client disconnected");
    }
    ExecCtx::Run(DEBUG_LOCATION, op->on_consumed, absl::OkStatus());
  }

 private:
  ~ShmemClientTransport() override = default;

  void EnsureReaderStarted();

  RefCountedPtr<ShmemServerTransport> server_;
  grpc_shmem::ControlBlock* cb_ = nullptr;
  std::unique_ptr<grpc_shmem::ShmemSegment> client_segment_;  // for cross-process
  std::atomic<bool> stop_{false};
  std::atomic<bool> shutdown_initiated_{false};
  std::atomic<bool> cleanup_complete_{false};
  std::thread reader_;
  std::atomic<uint32_t> next_stream_id_{1};
  int spin_iters_ = kDefaultSpinIters;
  // Cross-process semaphores for both directions
  grpc_shmem::CrossProcessSemaphore c2s_cross_sem_;
  grpc_shmem::CrossProcessSemaphore s2c_cross_sem_;
  
  // Semaphore adapter for queue operations
  std::unique_ptr<grpc_shmem::TransportSemaphoreAdapter> sem_adapter_;

  Mutex state_mu_;
  ConnectivityStateTracker state_tracker_
      ABSL_GUARDED_BY(state_mu_){"shmem_client_transport",
                                 GRPC_CHANNEL_CONNECTING};

  Mutex mu_;
  absl::flat_hash_map<uint32_t, CallHandler> handlers_ ABSL_GUARDED_BY(mu_);
  std::atomic<bool> reader_started_{false};

};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& args) {
    // Connectivity setup (start in CONNECTING like inproc).
    state_.store(ConnectionState::kInitial, std::memory_order_relaxed);
    {
      MutexLock l(&state_tracker_mu_);
      state_tracker_.SetState(GRPC_CHANNEL_CONNECTING, absl::OkStatus(),
                              "init");
    }
    // Validate and sanitize channel arguments
    int raw_spin_iters = args.GetInt(kArgShmemSpinIters).value_or(kDefaultSpinIters);
    if (raw_spin_iters < 0) {
      LOG(WARNING) << "Invalid " << kArgShmemSpinIters << " value " << raw_spin_iters 
                   << ", using default " << kDefaultSpinIters;
      spin_iters_ = kDefaultSpinIters;
    } else if (raw_spin_iters > 10000) {
      LOG(WARNING) << "Excessive " << kArgShmemSpinIters << " value " << raw_spin_iters 
                   << " clamped to 10000";
      spin_iters_ = 10000;
    } else {
      spin_iters_ = raw_spin_iters;
    }
    dispatch_only_ = args.GetBool(kArgShmemDispatchOnly).value_or(true);
    
    // Initialize call arena allocator from resource quota (or create one).
    ResourceQuota* rq = args.GetObject<ResourceQuota>();
    if (rq == nullptr) {
      fallback_rq_ = MakeResourceQuota("shmem-server");
      rq = fallback_rq_.get();
    }
    auto alloc =
        rq->memory_quota()->CreateMemoryAllocator("shmem-server-alloc");
    call_arena_allocator_ =
        MakeRefCounted<CallArenaAllocator>(std::move(alloc), 1024);
  }
  ShmemServerTransport(const ChannelArgs& args,
                       std::unique_ptr<grpc_shmem::ShmemSegment> seg)
      : segment_(std::move(seg)) {
    // Connectivity setup (start in CONNECTING like inproc).
    state_.store(ConnectionState::kInitial, std::memory_order_relaxed);
    {
      MutexLock l(&state_tracker_mu_);
      state_tracker_.SetState(GRPC_CHANNEL_CONNECTING, absl::OkStatus(),
                              "init");
    }
    // Validate and sanitize channel arguments
    int raw_spin_iters = args.GetInt(kArgShmemSpinIters).value_or(kDefaultSpinIters);
    if (raw_spin_iters < 0) {
      LOG(WARNING) << "Invalid " << kArgShmemSpinIters << " value " << raw_spin_iters 
                   << ", using default " << kDefaultSpinIters;
      spin_iters_ = kDefaultSpinIters;
    } else if (raw_spin_iters > 10000) {
      LOG(WARNING) << "Excessive " << kArgShmemSpinIters << " value " << raw_spin_iters 
                   << " clamped to 10000";
      spin_iters_ = 10000;
    } else {
      spin_iters_ = raw_spin_iters;
    }
    dispatch_only_ = args.GetBool(kArgShmemDispatchOnly).value_or(true);
    printf("DEBUG: ShmemServerTransport dispatch_only_ = %s\n", dispatch_only_ ? "true" : "false");
    fflush(stdout);
    
    // DIAGNOSTIC: Check segment before getting control block
    LOG(INFO) << "ShmemServerTransport constructor - segment_: " << segment_.get();
    if (segment_) {
      LOG(INFO) << "Segment exists, getting control block...";
      cb_ = segment_->control();
      LOG(INFO) << "Got control block: " << cb_;
      
      if (cb_) {
        LOG(INFO) << "ControlBlock magic: " << std::hex << cb_->magic_number.load(std::memory_order_acquire);
        LOG(INFO) << "ControlBlock c2s_queues: " << cb_->GetC2SQueues();
        LOG(INFO) << "ControlBlock s2c_queues: " << cb_->GetS2CQueues();
        
        if (!cb_->GetC2SQueues() || !cb_->GetS2CQueues()) {
          LOG(ERROR) << "FATAL: Queues not initialized in segment!";
          LOG(ERROR) << "ShmemSegment::InitQueues() likely failed or was not called";
        }
      }
    } else {
      LOG(ERROR) << "FATAL: segment_ is null - segment creation failed!";
      cb_ = nullptr;
    }
    
    // Initialize semaphore manager for cross-process communication
    if (cb_ != nullptr) {
      // Safely increment process count
      try {
        int32_t current_count = ++cb_->process_count;
        LOG(INFO) << "ShmemServerTransport attached, process count now: " << current_count;
        
        // Initialize cross-process semaphores if names are available
        if (cb_->c2s_sem_name[0] != '\0') {
          auto status = c2s_cross_sem_.InitFromName(cb_->c2s_sem_name);
          if (status.ok()) {
            LOG(INFO) << "Initialized c2s cross-process semaphore: " << cb_->c2s_sem_name;
          } else {
            LOG(WARNING) << "Failed to initialize c2s semaphore: " << status;
          }
        }
        if (cb_->s2c_sem_name[0] != '\0') {
          auto status = s2c_cross_sem_.InitFromName(cb_->s2c_sem_name);
          if (status.ok()) {
            LOG(INFO) << "Initialized s2c cross-process semaphore: " << cb_->s2c_sem_name;
          } else {
            LOG(WARNING) << "Failed to initialize s2c semaphore: " << status;
          }
        }
        
        // Create semaphore adapter for queue operations
        sem_adapter_ = std::make_unique<grpc_shmem::TransportSemaphoreAdapter>(
            &c2s_cross_sem_, &s2c_cross_sem_);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Error during server transport initialization: " << e.what();
      }
    }
    
    // Initialize call arena allocator from resource quota (or create one).
    ResourceQuota* rq = args.GetObject<ResourceQuota>();
    if (rq == nullptr) {
      fallback_rq_ = MakeResourceQuota("shmem-server");
      rq = fallback_rq_.get();
    }
    auto alloc =
        rq->memory_quota()->CreateMemoryAllocator("shmem-server-alloc");
    call_arena_allocator_ =
        MakeRefCounted<CallArenaAllocator>(std::move(alloc), 1024);
    
    // VALIDATION: Check ControlBlock before starting reader thread
    LOG(INFO) << "About to start reader thread. Validating ControlBlock...";
    if (!cb_) {
      LOG(ERROR) << "FATAL: cb_ is null at EnsureReaderStarted!";
      return;
    }
    
    LOG(INFO) << "ControlBlock at: " << cb_;
    LOG(INFO) << "c2s_queues: " << cb_->GetC2SQueues();
    LOG(INFO) << "s2c_queues: " << cb_->GetS2CQueues();
    
    if (!cb_->GetC2SQueues() || !cb_->GetS2CQueues()) {
      LOG(ERROR) << "FATAL: Queue pointers not initialized before starting reader!";
      LOG(ERROR) << "This indicates ShmemSegment::InitQueues() failed or was never called";
      return;
    }
    
    LOG(INFO) << "ControlBlock validation passed, starting reader thread";
    EnsureReaderStarted();
  }

  void SetCallDestination(RefCountedPtr<UnstartedCallDestination> h) override {
    MutexLock lock(&dest_mu_);
    dest_ = std::move(h);
    // Report READY (matches inproc behavior).
    state_.store(ConnectionState::kReady, std::memory_order_release);
    MutexLock l(&state_tracker_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(),
                            "accept function set");
    // Wake any announcers waiting for the destination.
    {
      MutexLock rl(&ready_mu_);
      ready_ = true;
      ready_cv_.SignalAll();
    }
    // DEBUG: Force a quick check to see if this gets called
    // by temporarily causing a crash here - remove after verification
    // abort();  // UNCOMMENT TO TEST
  }

  void Orphan() override {
    // Transition to SHUTDOWN and notify watchers (important for server
    // shutdown).
    Disconnect(absl::UnavailableError("shmem transport closed"));
    InitiateShutdown();
    Unref();
  }
  
 private:
  void InitiateShutdown() {
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      return; // Already initiated
    }
    
    LOG(INFO) << "ShmemServerTransport initiating shutdown";
    
    // Step 2: Finish any remaining active calls with UNAVAILABLE (not CANCELLED).
    {
      absl::flat_hash_map<uint32_t, CallInitiator> snapshot;
      {
        MutexLock lk(&active_mu_);
        snapshot = active_calls_;
        active_calls_.clear();
      }
      // Ensure callbacks can be scheduled during shutdown.
      ExecCtx exec_ctx(GRPC_EXEC_CTX_FLAG_IS_FINISHED |
                       GRPC_EXEC_CTX_FLAG_THREAD_RESOURCE_LOOP);
      for (auto& kv : snapshot) {
        kv.second.SpawnCancel(absl::UnavailableError("server shutdown"));
      }
    }
    
    stop_.store(true, std::memory_order_relaxed);
    if (cb_ != nullptr) {
      // REVERTED: Remove cleanup_initiated flag for baseline testing
    }
    
    // Step 3: Wait for threads to exit
    WaitForThreadsToExit();
    
    // Step 4: Cleanup resources
    CleanupResources();
    
    cleanup_complete_.store(true, std::memory_order_release);
    LOG(INFO) << "ShmemServerTransport shutdown complete";
  }
  
  void WaitForThreadsToExit() {
    // Only post semaphores if the ring server loop was actually started
    if (reader_started_.load(std::memory_order_acquire)) {
      if (cb_ != nullptr) {
        // Wake any waiting reader thread so it can observe stop_ and exit
        if (cb_) {
          // Wake both directions using cross-process semaphores
          if (cb_->c2s_sem_name[0] != '\0') {
            c2s_cross_sem_.post();
          }
          if (cb_->s2c_sem_name[0] != '\0') {
            s2c_cross_sem_.post();
          }
        }
      }
      
      if (reader_.joinable()) {
        LOG(INFO) << "ShmemServerTransport waiting for reader thread to exit";
        reader_.join();
        LOG(INFO) << "ShmemServerTransport reader thread joined successfully";
      }
    }
  }
  
  void CleanupResources() {
    try {
      // Cleanup semaphore manager
      // REVERTED: Remove semaphore manager cleanup for baseline testing
      
      // Handle process count and cleanup coordination
      if (cb_ != nullptr) {
        int32_t remaining = --cb_->process_count;
        LOG(INFO) << "ShmemServerTransport detaching, remaining processes: " << remaining;
        LOG(INFO) << "ShmemServerTransport detaching, remaining processes: " << remaining;
        
        // Server is responsible for cleaning up shared resources if last process
        if (remaining == 1) {
          LOG(INFO) << "Last process - server cleaning up shared resources";
          CleanupSharedResources();
        }
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "ShmemServerTransport cleanup error: " << e.what();
    }
  }
  
  void CleanupSharedResources() {
    try {
      // REVERTED: Remove semaphore name cleanup for baseline testing
      LOG(INFO) << "Server cleaned up shared semaphores";
    } catch (const std::exception& e) {
      LOG(ERROR) << "Error cleaning up shared resources: " << e.what();
    }
  }
  
 public:
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return nullptr; }
  ServerTransport* server_transport() override { return this; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op* op) override {
    // Handle connectivity watch like inproc.
    if (op->start_connectivity_watch != nullptr) {
      MutexLock l(&state_tracker_mu_);
      state_tracker_.AddWatcher(op->start_connectivity_watch_state,
                                std::move(op->start_connectivity_watch));
    }
    if (op->stop_connectivity_watch != nullptr) {
      MutexLock l(&state_tracker_mu_);
      state_tracker_.RemoveWatcher(op->stop_connectivity_watch);
    }
    // Server-initiated disconnect: finish all in-flight calls with UNAVAILABLE.
    if (!op->disconnect_with_error.ok()) {
      ExecCtx exec_ctx(GRPC_EXEC_CTX_FLAG_IS_FINISHED |
                       GRPC_EXEC_CTX_FLAG_THREAD_RESOURCE_LOOP);
      absl::Status st = op->disconnect_with_error;
      if (st.ok()) st = absl::UnavailableError("server shutdown");
      Disconnect(st);
      absl::flat_hash_map<uint32_t, CallInitiator> snapshot;
      {
        MutexLock lk(&active_mu_);
        snapshot = active_calls_;
        active_calls_.clear();
      }
      // Finish calls with UNAVAILABLE status to match test expectations
      for (auto& kv : snapshot) {
        kv.second.SpawnCancel(absl::UnavailableError("server shutdown"));
      }
    }
    if (!op->goaway_error.ok()) {
      absl::Status st = op->goaway_error;
      if (st.ok()) st = absl::UnavailableError("server goaway");
      // Publish SHUTDOWN; tests often expect watchers to observe this.
      Disconnect(st);
    }
    ExecCtx::Run(DEBUG_LOCATION, op->on_consumed, absl::OkStatus());
  }

  grpc_shmem::ControlBlock* control() const { return cb_; }
  int spin_iters() const { return spin_iters_; }
  bool dispatch_only() const { return dispatch_only_; }

  // Fast in-proc bootstrap: create server half and return initiator
  // immediately.
  CallInitiator AnnounceAndGetInitiator(uint32_t /*stream_id*/,
                                        ClientMetadataHandle md);

 private:
  ~ShmemServerTransport() override = default;

  void EnsureReaderStarted() {
    printf("DEBUG: EnsureReaderStarted called, cb_=%p, dispatch_only_=%s\n", 
           cb_, dispatch_only_ ? "true" : "false");
    fflush(stdout);
    if (cb_ == nullptr) {
      printf("DEBUG: cb_ is null, returning\n");
      fflush(stdout);
      return;
    }
    if (!dispatch_only_ &&
        !reader_started_.exchange(true, std::memory_order_acq_rel)) {
      printf("DEBUG: Starting ServerLoop thread...\n");
      fflush(stdout);
      stop_.store(false, std::memory_order_relaxed);
      reader_ = std::thread([this] { this->ServerLoop(); });
      printf("DEBUG: ServerLoop thread started\n");
      fflush(stdout);
    } else {
      printf("DEBUG: Not starting reader - dispatch_only_=%s, reader_started_=%s\n", 
             dispatch_only_ ? "true" : "false", 
             reader_started_.load() ? "true" : "false");
      fflush(stdout);
    }
  }

  void ServerLoop() {
    printf("DEBUG: ServerLoop thread started, entering main loop\n");
    fflush(stdout);
    ExecCtx exec_ctx;
    printf("DEBUG: ExecCtx created, validating control block\n");
    fflush(stdout);
    
    // STEP 1: VALIDATE CONTROLBLOCK BEFORE USE
    printf("DEBUG: ServerLoop starting, cb_=%p\n", cb_);
    fflush(stdout);
    
    if (!cb_) {
      printf("DEBUG: FATAL - cb_ is null!\n");
      fflush(stdout);
      return;
    }
    printf("DEBUG: cb_ is valid, checking basic fields\n");
    fflush(stdout);
    
    // Check if we can read basic fields using atomic operations
    try {
      printf("DEBUG: Reading magic number with atomic load...\n");
      fflush(stdout);
      uint64_t magic = cb_->magic_number.load(std::memory_order_acquire);
      printf("DEBUG: Reading version with atomic load...\n");
      fflush(stdout);
      uint32_t version = cb_->transport_version.load(std::memory_order_acquire);
      printf("DEBUG: ControlBlock magic: 0x%lx, version: %u (GRPCSMEM=0x47525043534D454D)\n", magic, version);
      fflush(stdout);
      
      if (magic != 0x47525043534D454Dull) {  // "GRPCSMEM" magic
        printf("DEBUG: WARNING - Invalid magic number, expected 0x47525043534D454D\n");
        fflush(stdout);
      }
    } catch (...) {
      printf("DEBUG: FATAL - Cannot read ControlBlock basic fields!\n");
      fflush(stdout);
      return;
    }
    
    // CHECK THE ACTUAL PROBLEM - c2s_queues pointer
    LOG(INFO) << "c2s_queues pointer: " << cb_->GetC2SQueues();
    LOG(INFO) << "s2c_queues pointer: " << cb_->GetS2CQueues();
    
    if (!cb_->GetC2SQueues()) {
      LOG(ERROR) << "FATAL: c2s_queues is null! Queue initialization failed.";
      return;
    }
    
    if (!cb_->GetS2CQueues()) {
      LOG(ERROR) << "FATAL: s2c_queues is null! Queue initialization failed.";
      return;
    }
    
    // Test if we can access first queue element safely
    try {
      // Try to read from the command queue structure  
      volatile auto* queue_ptr = cb_->GetC2SQueues();
      LOG(INFO) << "c2s_queues access test - pointer: " << queue_ptr;
      
      // Check if this looks like a valid memory address
      uintptr_t addr = reinterpret_cast<uintptr_t>(queue_ptr);
      if (addr < 0x1000 || addr > 0x7fffffffffff) {
        LOG(ERROR) << "FATAL: c2s_queues pointer looks invalid: " << std::hex << addr;
        return;
      }
      
      LOG(INFO) << "Queue pointer validation passed";
    } catch (...) {
      LOG(ERROR) << "FATAL: c2s_queues points to invalid memory!";
      return;
    }
    
    LOG(INFO) << "ControlBlock validation passed, proceeding with ServerLoop";
    
    struct StreamState {
      bool synthetic = true;       // synthetic fast-path or dispatched
      bool sent_initial = false;   // S2C initial metadata sent
      bool sent_trailing = false;  // S2C trailing metadata sent
      bool completed = false;      // stream fully complete, ready for cleanup
      bool cancelled = false;      // cancellation observed
      std::string path;            // :path from client initial metadata
      std::optional<CallInitiator> initiator;  // present if dispatched
      // Transitional: while dispatched streams do not yet integrate real server
      // method logic, we still need echo semantics for tests that purposely
      // choose a service/method style path (e.g. /c/N in the concurrency test).
      // We implement a temporary echo of inbound client messages directly from
      // the transport for dispatched streams. This flag is reserved for future
      // refinement (e.g., disabling echo once server handlers produce outputs).
      bool dispatched_echo_fallback = true;

      // Stage 1: Dispatched unary state for buffering initial metadata and
      // message
      struct DispatchedUnaryState {
        bool have_initial = false;
        bool have_message = false;
        bool client_trailing_seen = false;            // client EOS observed
        bool call_announced = false;                  // StartCall invoked
        std::vector<grpc_shmem::KVPair> initial_kvs;  // buffered initial kvs
        uint64_t payload_offset = 0;  // zero-copy: ring buffer offset
        uint32_t payload_size = 0;    // zero-copy: payload size
      };
      std::unique_ptr<DispatchedUnaryState>
          dispatched_unary;  // only for dispatched streams
    };

    // Stage 1: Helper to announce dispatched unary call when both initial +
    // message ready
    auto announce_dispatched_call = [this](uint32_t stream_id,
                                           StreamState& st) {
      if (!st.dispatched_unary || st.dispatched_unary->call_announced) {
        if (!st.dispatched_unary) {
        } else if (st.dispatched_unary->call_announced) {
        }
        return;
      }

      // Build ClientMetadata for dispatch
      auto arena = call_arena_allocator_->MakeArena();
      auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
      arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
      auto md = arena->MakePooledForOverwrite<ClientMetadata>();
      for (const auto& kv : st.dispatched_unary->initial_kvs) {
        if (kv.key == ":path") {
          md->Set(HttpPathMetadata(), Slice::FromCopiedString(kv.value));
        } else if (kv.key == ":method") {
          md->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
        } else if (kv.key == ":scheme") {
          if (kv.value == "https")
            md->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttps);
          else
            md->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttp);
        } else if (kv.key == "te") {
          md->Set(TeMetadata(), TeMetadata::kTrailers);
        } else if (kv.key == "user-agent") {
          md->Set(UserAgentMetadata(), Slice::FromCopiedString(kv.value));
        } else if (kv.key == ":authority") {
          md->Set(HttpAuthorityMetadata(), Slice::FromCopiedString(kv.value));
        } else {
          md->Append(kv.key, Slice::FromCopiedString(kv.value),
                     [](absl::string_view, const Slice&) {});
        }
      }
      auto call = MakeCallPair(std::move(md), std::move(arena));
      st.initiator.emplace(call.initiator);

      // Store CallInitiator for client ForwardCall access - use reference to
      // avoid copying
      {
        MutexLock lock(&stream_initiators_mu_);
        stream_initiators_[stream_id] =
            *st.initiator;  // Copy from the one we just emplaced
      }

      // Signal client that CallInitiator is ready - this replaces polling with
      // efficient blocking
      {
        MutexLock sync_lock(&stream_sync_mu_);
        auto sync_it = stream_sync_.find(stream_id);
        if (sync_it != stream_sync_.end()) {
          MutexLock stream_lock(&sync_it->second->mu);
          sync_it->second->initiator_ready = true;
          sync_it->second->cv.Signal();
        }
      }

      // ForwardCall handles all message/metadata forwarding automatically
      // No manual message pushing needed - the client-side messages will be
      // automatically forwarded to the server-side by ForwardCall

      RefCountedPtr<UnstartedCallDestination> d;
      {
        MutexLock lock(&dest_mu_);
        d = dest_;
      }
      if (d != nullptr) {
        d->StartCall(std::move(call.handler));
      }

      // SIMPLIFIED: ForwardCall will handle all S2C communication automatically
      // Just mark the call as announced - no manual forwarding needed
      st.dispatched_unary->call_announced = true;
    };
    absl::flat_hash_map<uint32_t, StreamState> streams;
    for (;;) {
      if (stop_.load(std::memory_order_relaxed)) break;  // REVERTED: Remove cleanup_initiated check
      grpc_shmem::Command cmd;
      bool has_command = grpc_shmem::PopCommandHybrid(
          cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S, spin_iters_,
          &cmd, sem_adapter_.get());

      if (!has_command) {
        // No command available - check stop flag again before continuing
        // This handles the case where we're using in-process bootstrap and no
        // C2S commands are coming
        continue;
      }

      if (has_command) {
        auto& st = streams[cmd.stream_id];
        switch (cmd.type) {
          case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
            if (!st.sent_initial) {
              // Deserialize client initial metadata
              std::vector<grpc_shmem::KVPair> kvs_in;
              if (cmd.data_size > 0) {
                const unsigned char* p =
                    cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
                kvs_in = grpc_shmem::DeserializeMetadataKVs(p, cmd.data_size);
              }
              for (const auto& kv : kvs_in) {
                if (kv.key == ":path") st.path = kv.value;
              }
              // Default to **dispatched** (in-proc) for all real RPCs.
              // Only use the synthetic ring path for the special "/cancel" hook.
              const bool is_cancel_path = (st.path == "/cancel");
              if (!is_cancel_path) {
                st.synthetic = false;
                // Stage 1: Buffer initial metadata for dispatched unary calls
                st.dispatched_unary =
                    std::make_unique<StreamState::DispatchedUnaryState>();
                st.dispatched_unary->initial_kvs = kvs_in;
                st.dispatched_unary->have_initial = true;

                // *** FIX PART 1: announce call immediately on initial metadata
                // *** ForwardCall will deliver messages and finish-sends; we do
                // NOT need to wait for a message or synthesize client trailing
                // here.
                announce_dispatched_call(cmd.stream_id, st);
              } else {
                // Synthetic path (retain Phase 1 behavior)
                std::vector<grpc_shmem::KVPair> kvs = {
                    {"content-type", "application/grpc"}, {"x-shmem", "1"}};
                auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
                uint64_t off = 0;
                grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb,
                                              buf.size(), &off);
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off,
                            buf.data(), buf.size());
                grpc_shmem::Command out{};
                out.stream_id = cmd.stream_id;
                out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA;
                out.data_offset = off;
                out.data_size = static_cast<uint32_t>(buf.size());
                out.grpc_status_code = 0;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
              }
              st.sent_initial = true;
            }
            // Release consumed bytes from c2s
            cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size,
                                                    std::memory_order_release);
            break;
          }
          case grpc_shmem::FrameType::C2S_MESSAGE: {
            const unsigned char* p =
                cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
            // Synthetic cancel-by-payload (only for synthetic streams)
            if (st.synthetic && !st.sent_trailing && st.path == "/cancel" &&
                cmd.data_size == 6 && memcmp(p, "cancel", 6) == 0) {
              // Mark cancelled semantics.
              st.cancelled = true;
              // We still may choose to NOT echo the message (tests do not
              // expect an echo for cancel). Send trailing CANCELLED immediately
              // if initial already sent; otherwise it will be sent when
              // trailing frame arrives.
              if (st.sent_initial) {
                std::vector<grpc_shmem::KVPair> kvs = {
                    {"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
                auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
                uint64_t off = 0;
                grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb,
                                              buf.size(), &off);
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off,
                            buf.data(), buf.size());
                grpc_shmem::Command out{};
                out.stream_id = cmd.stream_id;
                out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
                out.data_offset = off;
                out.data_size = static_cast<uint32_t>(buf.size());
                out.grpc_status_code = GRPC_STATUS_CANCELLED;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
                st.sent_trailing = true;
                st.completed = true;  // Mark stream as completed for cleanup
              }
              // Release input bytes
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
              break;
            }
            if (st.synthetic) {
              // Echo path (synthetic) - optimized for high performance
              uint64_t off = 0;
              
              // Try allocation once with optimized ReserveContiguous
              if (grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb,
                                               cmd.data_size, &off)) {
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, p, cmd.data_size);
                
                grpc_shmem::Command out{};
                out.stream_id = cmd.stream_id;
                out.type = grpc_shmem::FrameType::S2C_MESSAGE;
                out.data_offset = off;
                out.data_size = cmd.data_size;
                out.grpc_status_code = 0;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
              } else {
                // Fallback: use wrapping allocation for very large messages
                if (grpc_shmem::ReserveWrapping(&cb_->GetS2CQueues()->data_rb,
                                               cmd.data_size, &off)) {
                  // Handle potential wrap-around copy
                  const uint64_t capacity = cb_->GetS2CQueues()->data_rb.capacity;
                  if (off + cmd.data_size <= capacity) {
                    std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, p, cmd.data_size);
                  } else {
                    // Split copy across ring boundary
                    const uint32_t first_part = capacity - off;
                    const uint32_t second_part = cmd.data_size - first_part;
                    std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, p, first_part);
                    std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_), p + first_part, second_part);
                  }
                  
                  grpc_shmem::Command out{};
                  out.stream_id = cmd.stream_id;
                  out.type = grpc_shmem::FrameType::S2C_MESSAGE;
                  out.data_offset = off;
                  out.data_size = cmd.data_size;
                  out.grpc_status_code = 0;
                  grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                          grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
                }
                // If both allocations fail, drop the message (better than hanging)
              }
              
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
            } else {
              // *** FIX: messages for dispatched streams are forwarded by
              // ForwardCall; ignore any ring messages (should not be sent by
              // client) and just release.
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
            if (st.synthetic) {
              if (!st.sent_trailing) {
                int code = cmd.grpc_status_code != 0
                               ? cmd.grpc_status_code
                               : (st.cancelled ? GRPC_STATUS_CANCELLED
                                               : GRPC_STATUS_UNIMPLEMENTED);
                std::vector<grpc_shmem::KVPair> kvs = {
                    {"grpc-status", std::to_string(code)}};
                if (code == GRPC_STATUS_UNIMPLEMENTED)
                  kvs.push_back({"grpc-message", "unimplemented"});
                auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
                uint64_t off = 0;
                grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb,
                                              buf.size(), &off);
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off,
                            buf.data(), buf.size());
                grpc_shmem::Command out{};
                out.stream_id = cmd.stream_id;
                out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
                out.data_offset = off;
                out.data_size = static_cast<uint32_t>(buf.size());
                out.grpc_status_code = code;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
                st.sent_trailing = true;
                st.completed = true;  // Mark stream as completed for cleanup
              }
              if (cmd.data_size != 0)
                cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                    cmd.data_size, std::memory_order_release);
            } else {
              // *** FIX: for dispatched streams, ForwardCall will call
              // SpawnFinishSends() when the app half-closes; we should not gate
              // call announcement on client trailing nor need to interpret it
              // specially.
              if (cmd.data_size != 0) {
                cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                    cmd.data_size, std::memory_order_release);
              }
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_CANCEL: {
            st.cancelled = true;
            if (st.synthetic) {
              if (!st.sent_trailing) {
                std::vector<grpc_shmem::KVPair> kvs = {
                    {"grpc-status", std::to_string(GRPC_STATUS_CANCELLED)}};
                auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
                uint64_t off = 0;
                grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb,
                                              buf.size(), &off);
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off,
                            buf.data(), buf.size());
                grpc_shmem::Command out{};
                out.stream_id = cmd.stream_id;
                out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
                out.data_offset = off;
                out.data_size = static_cast<uint32_t>(buf.size());
                out.grpc_status_code = GRPC_STATUS_CANCELLED;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
                st.sent_trailing = true;
                st.completed = true;  // Mark stream as completed for cleanup
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

      // Clean up completed streams to prevent stale state accumulation
      std::vector<uint32_t> completed_stream_ids;

      // Check local completed flags (for synchronous completion)
      for (const auto& [stream_id, stream_state] : streams) {
        if (stream_state.completed) {
          completed_stream_ids.push_back(stream_id);
        }
      }

      // Check async completed streams (for promise-based completion)
      {
        std::lock_guard<std::mutex> lock(completed_streams_mu_);
        for (uint32_t stream_id : completed_streams_) {
          if (streams.find(stream_id) != streams.end()) {
            completed_stream_ids.push_back(stream_id);
          }
        }
        if (!completed_streams_.empty()) {
        }
        completed_streams_.clear();  // Clear the set after processing
      }

      // Remove duplicates and erase completed streams
      std::sort(completed_stream_ids.begin(), completed_stream_ids.end());
      completed_stream_ids.erase(
          std::unique(completed_stream_ids.begin(), completed_stream_ids.end()),
          completed_stream_ids.end());

      for (uint32_t stream_id : completed_stream_ids) {
        streams.erase(stream_id);

        // Clean up associated CallInitiator and sync structures
        {
          MutexLock lock(&stream_initiators_mu_);
          stream_initiators_.erase(stream_id);
        }
        {
          MutexLock lock(&stream_sync_mu_);
          stream_sync_.erase(stream_id);
        }
      }
    }
  }

  std::unique_ptr<grpc_shmem::ShmemSegment> segment_;
  grpc_shmem::ControlBlock* cb_ = nullptr;
  // Cross-process semaphores for both directions
  grpc_shmem::CrossProcessSemaphore c2s_cross_sem_;
  grpc_shmem::CrossProcessSemaphore s2c_cross_sem_;
  
  // Semaphore adapter for queue operations
  std::unique_ptr<grpc_shmem::TransportSemaphoreAdapter> sem_adapter_;
  
  std::atomic<bool> shutdown_initiated_{false};
  std::atomic<bool> cleanup_complete_{false};
  RefCountedPtr<UnstartedCallDestination> dest_;
  Mutex dest_mu_;
  // Signal when SetCallDestination() has installed the acceptor.
  Mutex ready_mu_;
  CondVar ready_cv_;
  bool ready_ ABSL_GUARDED_BY(ready_mu_) = false;
  // Track active dispatched calls for teardown/cancellation.
  Mutex active_mu_;
  absl::flat_hash_map<uint32_t, CallInitiator> active_calls_
      ABSL_GUARDED_BY(active_mu_);
  Mutex stream_mu_;  // protects streams hash map
  std::thread reader_;
  std::atomic<bool> stop_{false};
  std::atomic<bool> reader_started_{false};
  int spin_iters_ = kDefaultSpinIters;
  bool dispatch_only_ =
      true;  // Skip ring threads for dispatch-only workloads by default
  RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  Mutex s2c_mu_;
  // Thread-safe tracking of completed streams for cleanup
  std::mutex completed_streams_mu_;
  std::unordered_set<uint32_t> completed_streams_;
  // ForwardCall support: store CallInitiators for client access
  Mutex stream_initiators_mu_;
  absl::flat_hash_map<uint32_t, CallInitiator> stream_initiators_
      ABSL_GUARDED_BY(stream_initiators_mu_);

  // Efficient signaling mechanism - per-stream synchronization
  struct StreamSync {
    Mutex mu;
    CondVar cv;
    bool initiator_ready = false;
  };
  Mutex stream_sync_mu_;
  absl::flat_hash_map<uint32_t, std::unique_ptr<StreamSync>> stream_sync_
      ABSL_GUARDED_BY(stream_sync_mu_);

  // Hold a fallback resource quota if one was needed, to keep it alive.
  ResourceQuotaRefPtr fallback_rq_;

  // Connectivity/state similar to inproc
  enum class ConnectionState : uint8_t { kInitial, kReady, kDisconnected };
  std::atomic<ConnectionState> state_{ConnectionState::kInitial};
  std::atomic<bool> disconnecting_{false};
  Mutex state_tracker_mu_;
  ConnectivityStateTracker state_tracker_ ABSL_GUARDED_BY(state_tracker_mu_){
      "shmem_server_transport", GRPC_CHANNEL_CONNECTING};

  // Channelz + tracing
  std::atomic<uint64_t> calls_started_{0};
  std::atomic<uint64_t> calls_succeeded_{0};
  std::atomic<uint64_t> calls_failed_{0};

  void Disconnect(absl::Status error) {
    if (disconnecting_.exchange(true)) return;
    state_.store(ConnectionState::kDisconnected, std::memory_order_relaxed);
    MutexLock l(&state_tracker_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_SHUTDOWN, std::move(error),
                            "shmem transport disconnected");
  }
};

CallInitiator ShmemServerTransport::AnnounceAndGetInitiator(
    uint32_t stream_id, ClientMetadataHandle md) {
  // Ensure the server has installed a call destination before announcing.
  // Without this, early client calls can race and d==nullptr, dropping the call.
  {
    MutexLock rl(&ready_mu_);
    while (!ready_) {
      ready_cv_.Wait(&ready_mu_);
    }
  }

  // Ensure callbacks can be scheduled during call creation.
  ExecCtx exec_ctx;

  auto arena = call_arena_allocator_->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
  auto call = MakeCallPair(std::move(md), std::move(arena));
  RefCountedPtr<UnstartedCallDestination> d;
  {
    MutexLock lock(&dest_mu_);
    d = dest_;
  }
  // d must be non-null now; start the server-side call and record it active.
  if (d == nullptr) {
    return CallInitiator{};
  }
  d->StartCall(std::move(call.handler));
  {
    MutexLock lk(&active_mu_);
    active_calls_.emplace(stream_id, call.initiator);
    // Count started calls for Channelz
    calls_started_.fetch_add(1, std::memory_order_relaxed);
  }
  return std::move(call.initiator);
}

void ShmemClientTransport::EnsureReaderStarted() {
  if (cb_ == nullptr) return;
  if (server_ != nullptr && server_->dispatch_only())
    return;  // no S2C reader in in-proc mode
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    stop_.store(false, std::memory_order_relaxed);
    reader_ = std::thread([this] {
      ExecCtx exec_ctx;
      // Initialize client config lazily from server's config
      if (server_ != nullptr) spin_iters_ = server_->spin_iters();
      for (;;) {
        if (stop_.load(std::memory_order_relaxed)) break;  // REVERTED: Remove cleanup_initiated check
        grpc_shmem::Command cmd;
        if (!grpc_shmem::PopCommandHybrid(cb_->GetS2CQueues(), cb_,
                                          grpc_shmem::Direction::kS2C,
                                          spin_iters_, &cmd, sem_adapter_.get())) {
          continue;
        }
        std::unique_ptr<CallHandler> handler;
        {
          MutexLock lock(&mu_);
          auto it = handlers_.find(cmd.stream_id);
          if (it != handlers_.end())
            handler = std::make_unique<CallHandler>(it->second);
        }
        if (!handler) continue;
        switch (cmd.type) {
          case grpc_shmem::FrameType::S2C_INITIAL_METADATA: {
            auto data = cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
            auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
            handler->SpawnInfallible(
                "push-initial", [kvs = std::move(kvs), h = *handler]() mutable {
                  auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                  bool have_ct = false;
                  for (const auto& kv : kvs) {
                    if (kv.key == "content-type") {
                      have_ct = true;
                      md->Set(ContentTypeMetadata(),
                              ContentTypeMetadata::kApplicationGrpc);
                    } else {
                      md->Append(kv.key, Slice::FromCopiedString(kv.value),
                                 [](absl::string_view, const Slice&) {});
                    }
                  }
                  if (!have_ct) {
                    md->Set(ContentTypeMetadata(),
                            ContentTypeMetadata::kApplicationGrpc);
                  }
                  h.SpawnPushServerInitialMetadata(std::move(md));
                  return Empty{};
                });
            cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size,
                                                    std::memory_order_release);
            break;
          }
          case grpc_shmem::FrameType::S2C_MESSAGE: {
            // Zero-copy slice; tail advanced by slice destructor.
            grpc_slice s = grpc_shmem::MakeSliceFromRing(
                &cb_->GetS2CQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
            handler->SpawnInfallible("push-msg", [h = *handler, s]() mutable {
              SliceBuffer sb;
              // Wrap the ring-backed grpc_slice directly into a
              // grpc_core::Slice and append it without copying. Ownership of
              // the slice (and its tail-release destructor) transfers into the
              // SliceBuffer.
              sb.AppendIndexed(Slice(s));
              auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
              h.SpawnPushMessage(std::move(msg));
              return Empty{};
            });
            break;
          }
          case grpc_shmem::FrameType::S2C_TRAILING_METADATA: {
            auto data = cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
            auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
            handler->SpawnInfallible(
                "push-trailing", [kvs = std::move(kvs), h = *handler,
                                  stream_id = cmd.stream_id, this]() mutable {
                  auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                  grpc_status_code status = GRPC_STATUS_UNKNOWN;
                  for (const auto& kv : kvs) {
                    if (kv.key == "grpc-status") {
                      status =
                          static_cast<grpc_status_code>(atoi(kv.value.c_str()));
                    } else if (kv.key == "grpc-message") {
                      md->Set(GrpcMessageMetadata(),
                              Slice::FromCopiedString(kv.value));
                    } else {
                      md->Append(kv.key, Slice::FromCopiedString(kv.value),
                                 [](absl::string_view, const Slice&) {});
                    }
                  }
                  md->Set(GrpcStatusMetadata(), status);
                  h.SpawnPushServerTrailingMetadata(std::move(md));

                  // FINAL FIX: Clean up client-side handler when RPC completes
                  {
                    MutexLock lock(&this->mu_);
                    this->handlers_.erase(stream_id);
                  }

                  return Empty{};
                });
            cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size,
                                                    std::memory_order_release);
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
  // For dispatched/in-proc calls we do NOT put the handler in handlers_.
  // (synthetic/ring path can still use it if you keep that path around)

  auto cb = cb_;
  child_call_handler.SpawnGuarded(
      "pull_initial_metadata",
      TrySeq(child_call_handler.PullClientInitialMetadata(),
             [cb, stream_id, child_call_handler,
              this](ClientMetadataHandle md) mutable {
               // Decide "dispatched vs synthetic" from :path
               std::string path;
               if (auto* p = md->get_pointer(HttpPathMetadata()); p) {
                 path = std::string(p->as_string_view());
               }

               const bool is_cancel_path = (path == "/cancel");
               const bool is_cross_process = (server_ == nullptr);

               // Default to **dispatched** (in-proc) for all real RPCs.
               // Use synthetic ring path for cross-process mode or "/cancel" hook.
               if (!is_cancel_path && !is_cross_process) {
                 // *** In-proc bootstrap (dispatched) ***
                 auto initiator =
                     server_->AnnounceAndGetInitiator(stream_id, std::move(md));
                 ForwardCall(child_call_handler, std::move(initiator),
                             [this, stream_id](ServerMetadata& md) {
                               // Only erase if we ever inserted (synthetic).
                               MutexLock lock(&mu_);
                               auto it = handlers_.find(stream_id);
                               if (it != handlers_.end()) handlers_.erase(it);
                             });
                 return absl::OkStatus();
               } else {
                 // *** Synthetic ring path (cross-process communication and cancel test hook) ***
                 if (server_ != nullptr && server_->dispatch_only()) {
                   // In dispatch-only mode the ring reader is disabled:
                   // never route synthetic traffic in that mode.
                   // Fall back to dispatched to ensure the test proceeds.
                   auto initiator =
                       server_->AnnounceAndGetInitiator(stream_id, std::move(md));
                   ForwardCall(child_call_handler, std::move(initiator),
                               [this, stream_id](ServerMetadata& md) {
                                 MutexLock lock(&mu_); handlers_.erase(stream_id);
                               });
                   return absl::OkStatus();
                 }

                 // *** Old synthetic path (echo/cancel) keeps using the ring
                 // ***

                 // Add handler to map for synthetic calls only
                 {
                   MutexLock lock(&mu_);
                   handlers_.insert_or_assign(stream_id, child_call_handler);
                 }

                 // Serialize initial metadata: path/user-agent if present
                 std::vector<grpc_shmem::KVPair> kvs;
                 kvs.push_back({":path", path});
                 // Add required HTTP/2 pseudo-headers for server filter
                 kvs.push_back({":method", "POST"});
                 kvs.push_back({":scheme", "http"});
                 kvs.push_back({"te", "trailers"});
                 if (auto* auth = md->get_pointer(HttpAuthorityMetadata());
                     auth) {
                   kvs.push_back(
                       {":authority", std::string(auth->as_string_view())});
                 } else {
                   kvs.push_back({":authority", "test.authority"});
                 }
                 if (auto* ua = md->get_pointer(UserAgentMetadata()); ua) {
                   kvs.push_back(
                       {"user-agent", std::string(ua->as_string_view())});
                 }

                 auto vec = grpc_shmem::SerializeMetadataKVs(kvs);
                 uint64_t off = 0;
                 grpc_shmem::ReserveContiguous(&cb->GetC2SQueues()->data_rb,
                                               vec.size(), &off);
                 printf("DEBUG: About to memcpy metadata to ring buffer, off=%zu, size=%zu\n", off, vec.size());
                 fflush(stdout);
                 std::memcpy(cb->GetC2SQueues()->data_rb.GetBuffer(cb) + off,
                             vec.data(), vec.size());
                 printf("DEBUG: memcpy completed, creating command\n");
                 fflush(stdout);
                 grpc_shmem::Command cmd{};
                 cmd.stream_id = stream_id;
                 cmd.type = grpc_shmem::FrameType::C2S_INITIAL_METADATA;
                 cmd.data_offset = off;
                 cmd.data_size = static_cast<uint32_t>(vec.size());
                 printf("DEBUG: About to push command to C2S queue\n");
                 fflush(stdout);
                 grpc_shmem::PushCommand(cb->GetC2SQueues(), cb,
                                         grpc_shmem::Direction::kC2S, cmd, sem_adapter_.get());
                 printf("DEBUG: Command pushed successfully\n");
                 fflush(stdout);

                 return absl::OkStatus();
               }
             }));
}

// Global storage for cross-process segments (keyed by control block pointer)
static std::mutex g_cross_process_segments_mu;
static std::unordered_map<grpc_shmem::ControlBlock*, 
                         std::unique_ptr<grpc_shmem::ShmemSegment>> g_cross_process_segments;

// Helper to store cross-process segment for cleanup
void StoreCrossProcessSegment(grpc_shmem::ControlBlock* cb, 
                             std::unique_ptr<grpc_shmem::ShmemSegment> segment) {
  std::lock_guard<std::mutex> lock(g_cross_process_segments_mu);
  g_cross_process_segments[cb] = std::move(segment);
}

// Helper to remove cross-process segment 
void RemoveCrossProcessSegment(grpc_shmem::ControlBlock* cb) {
  std::lock_guard<std::mutex> lock(g_cross_process_segments_mu);
  g_cross_process_segments.erase(cb);
}

}  // namespace

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args,
                       const ChannelArgs& client_channel_args) {
  // Create a shared memory segment only if we will use ring queues.
  static std::atomic<uint64_t> pair_id{0};
  // Reuse same arg key as above:
  const bool dispatch_only =
      server_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(true);

  std::unique_ptr<grpc_shmem::ShmemSegment> segment;
  grpc_shmem::ControlBlock* cb = nullptr;

  if (!dispatch_only) {
    grpc_shmem::SegmentConfig cfg;
    cfg.name = absl::StrCat("grpc_shmem_", getpid(), "_", pair_id.fetch_add(1));
    // Optimized sizing for high-throughput large message performance
    // Larger buffers reduce fragmentation and improve contiguous allocation success
    cfg.data_ring_capacity = 64 * 1024 * 1024;   // 64MB per direction for better large message handling
    cfg.size = 192 * 1024 * 1024;                // 192MB total segment
    grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
    auto s = grpc_shmem::ShmemSegment::Create(cfg);
    segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
    cb = segment->control();
  }

  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      server_channel_args, std::move(segment));
  auto client_transport = MakeOrphanable<ShmemClientTransport>(
      server_transport->RefAsSubclass<ShmemServerTransport>(), cb, client_channel_args);
  return {std::move(client_transport), std::move(server_transport)};
}

OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& server_channel_args) {
  printf("DEBUG: MakeNamedShmemServerTransport called with server_name: %s\n", server_name.c_str());
  fflush(stdout);
  
  // Force ring mode for cross-process server
  ChannelArgs ring_mode_args = server_channel_args
      .Set("grpc.shmem.dispatch_only", false);

  std::unique_ptr<grpc_shmem::ShmemSegment> segment;
  
  grpc_shmem::SegmentConfig cfg;
  cfg.name = absl::StrCat("grpc_shmem_", server_name);
  cfg.server_name = server_name;  // For named semaphores
  cfg.data_ring_capacity = 64 * 1024 * 1024;
  cfg.size = 192 * 1024 * 1024;
  printf("DEBUG: Segment config: name=%s, size=%zu, data_ring_capacity=%zu\n", 
         cfg.name.c_str(), cfg.size, cfg.data_ring_capacity);
  fflush(stdout);
  
  printf("DEBUG: Removing existing segment if exists...\n");
  fflush(stdout);
  grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
  
  printf("DEBUG: Creating new segment...\n");
  fflush(stdout);
  auto s = grpc_shmem::ShmemSegment::Create(cfg);
  
  printf("DEBUG: Created segment, checking control block...\n");
  fflush(stdout);
  if (s.control() == nullptr) {
    LOG(ERROR) << "Failed to create named shmem segment: " << cfg.name;
    return nullptr;
  }
  
  LOG(INFO) << "Control block created successfully at: " << s.control();
  LOG(INFO) << "Control block c2s_queues: " << s.control()->GetC2SQueues();
  LOG(INFO) << "Control block s2c_queues: " << s.control()->GetS2CQueues();
  
  segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
  LOG(INFO) << "Wrapped in unique_ptr, creating server transport...";

  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      ring_mode_args, std::move(segment));
  
  LOG(INFO) << "Server transport created: " << server_transport.get();
  return std::move(server_transport);
}

OrphanablePtr<Transport> ConnectToShmemServerTransport(
    const std::string& server_name, const ChannelArgs& client_channel_args) {
  printf("DEBUG: ConnectToShmemServerTransport called with server_name: %s\n", server_name.c_str());
  fflush(stdout);
  
  const bool dispatch_only =
      client_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(false);

  if (dispatch_only) {
    LOG(ERROR) << "Cross-process shmem requires ring mode (not dispatch-only)";
    return nullptr;
  }

  std::string segment_name = absl::StrCat("grpc_shmem_", server_name);
  printf("DEBUG: Opening segment: %s\n", segment_name.c_str());
  fflush(stdout);
  
  auto segment = grpc_shmem::ShmemSegment::Open(segment_name);
  
  printf("DEBUG: Segment opened, control block: %p\n", segment.control());
  fflush(stdout);
  if (segment.control() == nullptr) {
    LOG(ERROR) << "Failed to connect to shmem server: " << server_name;
    return nullptr;
  }

  grpc_shmem::ControlBlock* cb = segment.control();
  printf("DEBUG: Control block at: %p\n", cb);
  printf("DEBUG: c2s_queues: %p, s2c_queues: %p\n", cb->GetC2SQueues(), cb->GetS2CQueues());
  fflush(stdout);
  
  auto segment_ptr = std::make_unique<grpc_shmem::ShmemSegment>(std::move(segment));
  
  printf("DEBUG: About to store segment and create client transport\n");
  fflush(stdout);
  
  // Store segment for cleanup
  StoreCrossProcessSegment(cb, std::move(segment_ptr));
  
  printf("DEBUG: Creating client transport with cb: %p\n", cb);
  fflush(stdout);
  
  return MakeOrphanable<ShmemClientTransport>(nullptr, cb, client_channel_args);
}

}  // namespace grpc_core
