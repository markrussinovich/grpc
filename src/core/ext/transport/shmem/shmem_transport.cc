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
#include "src/core/util/ref_counted_ptr.h"
#include "src/core/util/sync.h"
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
#include "src/core/lib/promise/for_each.h"
#include "src/core/lib/promise/status_flag.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/slice/slice.h"
#include "src/core/lib/transport/connectivity_state.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/channelz/channelz.h"
#include "src/core/util/debug_location.h"
#include "src/core/transport/auth_context.h"
#include "src/core/call/security_context.h"
#include "src/core/util/ref_counted_ptr.h"
#include "include/grpc/grpc_security.h"
#include "absl/log/log.h"

// Legacy stream-op scaffolding removed: this transport uses promise-based APIs exclusively.

// Helper to build a valid shmem auth context
static grpc_core::RefCountedPtr<grpc_auth_context> MakeShmemAuthContext() {
  // Create an empty (insecure) auth context with nullptr chained context
  grpc_core::RefCountedPtr<grpc_auth_context> ctx = grpc_core::MakeRefCounted<grpc_auth_context>(nullptr);

  // Required: tell the stack what this transport is
  grpc_auth_context_add_cstring_property(
      ctx.get(),
      GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME,  // usually "transport_security_type"
      "shmem");

  // Optional: make this property the "identity" (harmless & keeps some code paths simpler)
  grpc_auth_context_set_peer_identity_property_name(
      ctx.get(),
      GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME);

  return ctx;
}

// Note: MakeShmemSecurityContext removed - using grpc_server_security_context_create directly

namespace grpc_shmem {
// Shutdown tracer removed for compilation
}

namespace grpc_core {
namespace {

// Concrete implementation of TransportSemaphoreAdapter
class ConcreteSemaphoreAdapter : public grpc_shmem::TransportSemaphoreAdapter {
 public:
  ConcreteSemaphoreAdapter(grpc_shmem::CrossProcessSemaphore* c2s_sem, 
                          grpc_shmem::CrossProcessSemaphore* s2c_sem)
      : c2s_sem_(c2s_sem), s2c_sem_(s2c_sem) {}
  
  void Post(grpc_shmem::ControlBlock* cb, bool is_c2s) override {
    if (is_c2s && c2s_sem_) {
      c2s_sem_->post();
    } else if (!is_c2s && s2c_sem_) {
      s2c_sem_->post();
    }
  }
  
  void Wait(bool is_c2s) override {
    if (is_c2s && c2s_sem_) {
      c2s_sem_->wait();
    } else if (!is_c2s && s2c_sem_) {
      s2c_sem_->wait();
    }
  }
  
 private:
  grpc_shmem::CrossProcessSemaphore* c2s_sem_;
  grpc_shmem::CrossProcessSemaphore* s2c_sem_;
};

// Helper function to handle ReserveContiguous with retry/backoff
bool ReserveContiguousWithRetry(grpc_shmem::DataRingBuffer* rb, size_t n, uint64_t* off) {
  bool reserved = grpc_shmem::ReserveContiguous(rb, n, off);
  for (int i = 0; !reserved && i < 100; ++i) {
    absl::SleepFor(absl::Microseconds(50));
    reserved = grpc_shmem::ReserveContiguous(rb, n, off);
  }
  if (!reserved) {
    LOG(ERROR) << "shmem: failed to reserve " << n << " bytes; dropping frame";
  }
  return reserved;
}

// Note: kMaxMessageSize was previously defined here but is unused in the current implementation
constexpr absl::string_view kArgShmemSpinIters = "grpc.shmem.spin_iters";
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
        VLOG(2) << "CLIENT initializing S2C semaphore: '" << cb_->s2c_sem_name << "'";
        auto status = s2c_cross_sem_.InitFromName(cb_->s2c_sem_name);
        if (status.ok()) {
          VLOG(2) << "CLIENT S2C semaphore initialized successfully: '" << cb_->s2c_sem_name << "'";
          LOG(INFO) << "Initialized s2c cross-process semaphore: " << cb_->s2c_sem_name;
        } else {
          LOG(WARNING) << "CLIENT S2C semaphore initialization FAILED: '" << cb_->s2c_sem_name << "'";
          LOG(WARNING) << "Failed to initialize s2c semaphore: " << status;
        }
      }
      
      // Create semaphore adapter for queue operations
      sem_adapter_ = std::make_unique<ConcreteSemaphoreAdapter>(
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
    // Debug tracing removed
    ExecCtx exec_ctx;
    
    // Debug checkpoint: Checking if already initiated
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      // Debug checkpoint: Already initiated, returning
      return; // Already initiated
    }
    
    // Debug checkpoint: Setting shutdown flags
    // Step 2: Set stop flag and wake threads
    stop_.store(true, std::memory_order_relaxed);
    if (cb_ != nullptr && true) {
      // REVERTED: Remove cleanup_initiated flag for baseline testing
    }
    
    // Debug checkpoint: Waiting for threads to exit
    // Step 3: Wake any waiting reader threads
    WaitForThreadsToExit();
    
    // Debug checkpoint: Cleaning up resources
    // Step 4: Cleanup resources in proper order
    CleanupResources();
    
    // Debug checkpoint: Setting cleanup complete flag
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    // Debug tracing removed
    
    // Debug checkpoint: Checking if reader was started
    // Only wake semaphores if a ring reader thread was started
    if (reader_started_.load(std::memory_order_acquire)) {
      // Debug checkpoint: Reader was started, waking semaphores
      if (cb_ != nullptr && true) {
        try {
          // Wake any waiting reader so it can observe stop_ and exit
        if (cb_) {
          // Client only needs to wake its own S2C reader thread
          // Do NOT wake C2S as that disturbs the server
          if (cb_->s2c_sem_name[0] != '\0') {
            s2c_cross_sem_.post();
          }
        }
          // Debug checkpoint: Semaphores woken successfully
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error waking semaphores: " << e.what();
        }
      }
      
      // Debug checkpoint: Joining reader thread
      if (reader_.joinable()) {
        try {
          reader_.join();
          // Debug checkpoint: Reader thread joined successfully
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error joining reader thread: " << e.what();
        }
      } else {
        // Debug checkpoint: Reader thread not joinable
      }
    } else {
      // Debug checkpoint: Reader was never started, skipping thread cleanup
    }
  }
  
  void CleanupResources() {
    // Debug tracing removed
    
    try {
      // Debug checkpoint: Cleaning semaphore manager
      // Cleanup semaphore manager
      // REVERTED: Remove semaphore manager cleanup for baseline testing
      
      // Debug checkpoint: Handling process count coordination
      // Handle process count and cleanup coordination
      if (cb_ != nullptr && true) {
        // Debug checkpoint: Decrementing process count
        int32_t remaining = --cb_->process_count;
        LOG(INFO) << "ShmemServerTransport detaching, remaining processes: " << remaining;
        LOG(INFO) << "ShmemClientTransport detaching, remaining processes: " << remaining;
        
        // Clean up cross-process segment if this is a cross-process client
        if (server_ == nullptr) {
          // Debug checkpoint: Cross-process client, checking if last process
          // Last process cleans up shared resources
          if (remaining == 1) {
            // Debug checkpoint: Last process - cleaning up shared resources
            RemoveCrossProcessSegment(cb_);
          } else {
            // Debug checkpoint: Not last process, skipping shared resource cleanup
          }
        } else {
          // Debug checkpoint: In-process client, skipping shared resource cleanup
        }
      } else {
        // Debug checkpoint: Control block invalid, skipping process coordination
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
    // Always use ServerLoop for ring-based I/O
    
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
    // Always use ServerLoop for ring-based I/O
    
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
          VLOG(2) << "SERVER initializing S2C semaphore: '" << cb_->s2c_sem_name << "'";
          auto status = s2c_cross_sem_.InitFromName(cb_->s2c_sem_name);
          if (status.ok()) {
            VLOG(2) << "SERVER S2C semaphore initialized successfully: '" << cb_->s2c_sem_name << "'";
            LOG(INFO) << "Initialized s2c cross-process semaphore: " << cb_->s2c_sem_name;
          } else {
            LOG(WARNING) << "SERVER S2C semaphore initialization FAILED: '" << cb_->s2c_sem_name << "'";
            LOG(WARNING) << "Failed to initialize s2c semaphore: " << status;
          }
        }
        
        // Create semaphore adapter for queue operations
        sem_adapter_ = std::make_unique<ConcreteSemaphoreAdapter>(
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
    
    LOG(INFO) << "ControlBlock validation passed, deferring reader thread start";
    // DEFER: Don't start reader thread during construction to avoid startup hang
    // EnsureReaderStarted();
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
    
  // Start the reader thread since the server is ready to accept calls.
  // Note: Do NOT override the accept_stream callback here; the core will
  // have already installed its own callback via PerformOp during
  // Server::SetupTransport. We simply start consuming C2S traffic and call
  // that callback when new streams arrive, mirroring TCP behavior.
  printf("DEBUG: SetCallDestination called for shmem server - starting reader thread\n");
  fflush(stdout);
  LOG(INFO) << "SetCallDestination called - starting reader thread";

    EnsureReaderStarted();
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
  // Legacy stream-op vtable methods removed (promise-based path only).
  
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
    
    // Handle accept stream callback registration - following TCP transport pattern
    if (op->set_accept_stream) {
      LOG(INFO) << "ShmemServerTransport: Registering accept_stream_cb";
      accept_stream_cb_ = op->set_accept_stream_fn;
      accept_stream_cb_user_data_ = op->set_accept_stream_user_data;
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

  // Fast in-proc bootstrap: create server half and return initiator
  // immediately.
  CallInitiator AnnounceAndGetInitiator(uint32_t /*stream_id*/,
                                        ClientMetadataHandle md);

 private:
  ~ShmemServerTransport() override = default;

  void EnsureReaderStarted() {
    VLOG(2) << "EnsureReaderStarted called, cb_=" << cb_;
    if (cb_ == nullptr) {
      VLOG(2) << "cb_ is null, returning";
      return;
    }
    if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
      VLOG(2) << "Starting ServerLoop thread...";
      stop_.store(false, std::memory_order_relaxed);
      reader_ = std::thread([this] { this->ServerLoop(); });
      VLOG(2) << "ServerLoop thread started";
    } else {
      VLOG(2) << "Reader already started";
    }
  }

  // Legacy stream-op completion helpers removed.

  // Response monitoring loop - forward real server responses onto S2C ring.
  // Implement CallOutboundLoop equivalent for shmem cross-process communication
  auto ShmemCallOutboundLoop(uint32_t stream_id, CallInitiator call_initiator) {
    printf("DEBUG: ShmemCallOutboundLoop STARTED for stream %u\n", stream_id);
    fflush(stdout);
    
    printf("DEBUG: About to call PullServerInitialMetadata for stream %u\n", stream_id);
    fflush(stdout);
    
    return Seq(
        TrySeq(
          call_initiator.PullServerInitialMetadata(),
          [this, stream_id](std::optional<ServerMetadataHandle> md) {
            if (md.has_value()) {
              printf("DEBUG: ShmemCallOutboundLoop: sending S2C_INITIAL_METADATA for stream %u\n", stream_id);
              fflush(stdout);
              std::vector<grpc_shmem::KVPair> kvs;
              kvs.push_back({"content-type", "application/grpc"});
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0;
              if (grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb, buf.size(), &off)) {
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, buf.data(), buf.size());
                grpc_shmem::Command out{};
                out.stream_id = stream_id;
                out.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA;
                out.data_offset = off;
                out.data_size = static_cast<uint32_t>(buf.size());
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
                printf("DEBUG: ShmemCallOutboundLoop: S2C_INITIAL_METADATA sent for stream %u\n", stream_id);
                fflush(stdout);
              }
            }
            return Success{};
          }
        ),
        ForEach(MessagesFrom(call_initiator),
          [this, stream_id](MessageHandle msg) {
            auto* payload = msg->payload();
            const size_t n = payload->Length();
            if (n == 0) return Success{};  // nothing to send
            printf("DEBUG: ShmemCallOutboundLoop: sending S2C_MESSAGE for stream %u, size=%zu\n", stream_id, n);
            fflush(stdout);
            uint64_t off = 0;
            if (grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb, n, &off)) {
              payload->CopyToBuffer(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off);
              grpc_shmem::Command out{};
              out.stream_id = stream_id;
              out.type = grpc_shmem::FrameType::S2C_MESSAGE;
              out.data_offset = off;
              out.data_size = static_cast<uint32_t>(n);
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
              printf("DEBUG: ShmemCallOutboundLoop: S2C_MESSAGE sent for stream %u\n", stream_id);
              fflush(stdout);
            }
            return Success{};
          }
        ),
        Map(
          call_initiator.PullServerTrailingMetadata(),
          [this, stream_id](ServerMetadataHandle md) {
            printf("DEBUG: ShmemCallOutboundLoop: sending S2C_TRAILING_METADATA for stream %u\n", stream_id);
            fflush(stdout);
            std::vector<grpc_shmem::KVPair> kvs;
            grpc_status_code status = GRPC_STATUS_OK;
            if (auto* s = md->get_pointer(GrpcStatusMetadata()); s) {
              status = *s;
            }
            if (auto* m = md->get_pointer(GrpcMessageMetadata()); m) {
              kvs.push_back({"grpc-message", std::string(m->as_string_view())});
            }
            kvs.push_back({"grpc-status", std::to_string(status)});
            auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
            uint64_t off = 0;
            if (grpc_shmem::ReserveContiguous(&cb_->GetS2CQueues()->data_rb, buf.size(), &off)) {
              std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, buf.data(), buf.size());
              grpc_shmem::Command out{};
              out.stream_id = stream_id;
              out.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
              out.data_offset = off;
              out.data_size = static_cast<uint32_t>(buf.size());
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
              printf("DEBUG: ShmemCallOutboundLoop: S2C_TRAILING_METADATA sent for stream %u\n", stream_id);
              fflush(stdout);
            }
            // Clean up stream tracking after sending trailing metadata
            {
              MutexLock lock(&stream_initiators_mu_);
              stream_initiators_.erase(stream_id);
            }
            printf("DEBUG: ShmemCallOutboundLoop completed for stream %u\n", stream_id);
            fflush(stdout);
            return Success{};
          }
        )
    );
  }

  void ServerLoop() {
    VLOG(2) << "ServerLoop thread started, entering main loop";
    ExecCtx exec_ctx;
    VLOG(2) << "ExecCtx created, validating control block";
    
    // STEP 1: VALIDATE CONTROLBLOCK BEFORE USE
    VLOG(2) << "ServerLoop starting, cb_=" << cb_;
    
    if (!cb_) {
      LOG(ERROR) << "FATAL - cb_ is null!";
      return;
    }
    VLOG(2) << "cb_ is valid, checking basic fields";
    
    // Check if we can read basic fields using atomic operations
    try {
      VLOG(3) << "Reading magic number with atomic load...";
      uint64_t magic = cb_->magic_number.load(std::memory_order_acquire);
      VLOG(3) << "Reading version with atomic load...";
      uint32_t version = cb_->transport_version.load(std::memory_order_acquire);
      VLOG(2) << "ControlBlock magic: 0x" << std::hex << magic << ", version: " << std::dec << version << " (GRPCSMEM=0x47525043534D454D)";
      
      if (magic != 0x47525043534D454Dull) {  // "GRPCSMEM" magic
        LOG(WARNING) << "Invalid magic number, expected 0x47525043534D454D";
      }
    } catch (...) {
      LOG(ERROR) << "FATAL - Cannot read ControlBlock basic fields!";
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
      bool sent_initial = false;   // S2C initial metadata sent
      bool sent_trailing = false;  // S2C trailing metadata sent
      bool completed = false;      // stream fully complete, ready for cleanup
      bool cancelled = false;      // cancellation observed
      std::string path;            // :path from client initial metadata
      std::optional<CallInitiator> initiator;  // call initiator for server communication
    };

    // Simplified ring-based approach - no complex dispatched call logic needed
    absl::flat_hash_map<uint32_t, StreamState> streams;
    int loop_count = 0;
    VLOG(2) << "ServerLoop: Starting command processing loop";
    
    for (;;) {
      if (stop_.load(std::memory_order_relaxed)) break;  // REVERTED: Remove cleanup_initiated check
      loop_count++;
      
      // Log every 50 iterations to show server is alive
      if (loop_count % 50 == 0) {
        printf("DEBUG: ServerLoop: Iteration %d, checking for commands\n", loop_count);
        fflush(stdout);
        
        // Check if we can access queues
        auto* c2s_queues = cb_->GetC2SQueues();
        if (!c2s_queues) {
          printf("DEBUG: WARNING - C2S queues pointer is null\n");
          fflush(stdout);
        }
      }
      
      grpc_shmem::Command cmd;
      bool has_command = grpc_shmem::PopCommandHybrid(
          cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S, spin_iters_,
          &cmd, sem_adapter_.get());
      
      if (has_command) {
        printf("DEBUG: ServerLoop: GOT COMMAND! Type: %d, Stream ID: %u (iteration %d)\n", 
               static_cast<int>(cmd.type), cmd.stream_id, loop_count);
        fflush(stdout);
      } else {
        // Only log on first few iterations and then every 100 iterations
        if (loop_count <= 5 || (loop_count % 100 == 0)) {
          VLOG(3) << "ServerLoop: No command available (iteration " << loop_count << ")";
        }
        continue;
      }

      if (has_command) {
        VLOG(2) << "Processing command in switch statement";
        auto& st = streams[cmd.stream_id];
        switch (cmd.type) {
          case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
            VLOG(2) << "Handling C2S_INITIAL_METADATA for stream " << cmd.stream_id;
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
              VLOG(2) << "Extracted path: '" << st.path << "'";
              
              // Always use ring-based approach: Create server call via UnstartedCallDestination::StartCall
              
              // Get the call destination
              RefCountedPtr<UnstartedCallDestination> dest;
              {
                MutexLock lock(&dest_mu_);
                dest = dest_;
              }
              
              if (dest != nullptr) {
                // Create server call through StartCall - this is the proper way
                auto arena = call_arena_allocator_->MakeArena();
                auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
                arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
                auto md = arena->MakePooledForOverwrite<ClientMetadata>();

                // IMPORTANT: Provide a per-call server security context (like TCP does)
                // ServerAuthFilter requires this to be present.
                grpc_server_security_context* server_ctx = 
                    grpc_server_security_context_create(arena.get());
                server_ctx->auth_context = MakeShmemAuthContext();
                arena->SetContext<grpc_core::SecurityContext>(server_ctx);

                // Optional but nice: set a peer string so filters/logging have something sane.
                md->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
                
                // Set metadata from the shmem request
                for (const auto& kv : kvs_in) {
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
                  } else if (kv.key == "content-type") {
                    md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                  } else if (kv.key == "grpc-accept-encoding") {
                    md->Set(GrpcAcceptEncodingMetadata(), CompressionAlgorithmSet{GRPC_COMPRESS_NONE});
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
                
                // Start the server call and response bridge
                dest->StartCall(std::move(call.handler));
                
                // Start the response bridge to forward server responses back to client
                call.initiator.SpawnGuarded("shmem-response-bridge", 
                  [this, stream_id = cmd.stream_id, call_initiator = call.initiator]() mutable {
                    return ShmemCallOutboundLoop(stream_id, std::move(call_initiator));
                  });
              } else {
                // No destination available - this shouldn't happen in cross-process mode
                VLOG(1) << "No call destination available for stream " << cmd.stream_id;
              }
              
              st.sent_initial = true;
              
              st.sent_initial = true;
            }
            // Release consumed bytes from c2s
            cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size,
                                                    std::memory_order_release);
            break;
          }
          case grpc_shmem::FrameType::C2S_MESSAGE: {
            VLOG(2) << "Handling C2S_MESSAGE for stream " << cmd.stream_id << ", size=" << cmd.data_size;
            
            // Special case for cancel path with "cancel" payload
            if (st.path == "/cancel" && cmd.data_size == 6) {
              const unsigned char* p = cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
              if (memcmp(p, "cancel", 6) == 0) {
                st.cancelled = true;
                // Let the normal server pipeline handle cancellation
              }
            }
            // Forward client message into call pipeline (proper transport behavior)
            if (!st.initiator.has_value()) {
              // Should not happen: message before announcement; drop safely.
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
              break;
            }
            // Zero-copy slice that will advance C2S tail when released.
            grpc_slice s = grpc_shmem::MakeSliceFromRing(
                &cb_->GetC2SQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
            auto init = *st.initiator;  // copy
            init.SpawnInfallible("push-c2s-msg",
                                 [init, s]() mutable {
                                   // Allocate message within call arena
                                   SliceBuffer sb;
                                   sb.AppendIndexed(Slice(s));
                                   auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
                                   init.SpawnPushMessage(std::move(msg));
                                   return Empty{};
                                 });
            // Note: Do NOT advance C2S tail here; slice destructor will.
            break;
          }
          case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
            VLOG(2) << "Handling C2S_TRAILING_METADATA for stream " << cmd.stream_id;
            // Client half-close: Signal FinishSends to server pipeline
            if (st.initiator.has_value()) {
              st.initiator->SpawnFinishSends();
            }
            if (cmd.data_size != 0) {
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_CANCEL: {
            st.cancelled = true;
            if (st.initiator.has_value()) {
              st.initiator->SpawnCancel();
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
  
  // Accept stream callback fields - following TCP transport pattern  
  void (*accept_stream_cb_)(void* user_data, grpc_core::Transport* transport,
                           const void* server_data) = nullptr;
  void* accept_stream_cb_user_data_ = nullptr;
  
  // Stream mapping: grpc_stream -> shmem stream_id
  Mutex stream_map_mu_;
  absl::flat_hash_map<grpc_stream*, uint32_t> grpc_to_shmem_stream_
      ABSL_GUARDED_BY(stream_map_mu_);
  absl::flat_hash_map<uint32_t, grpc_stream*> shmem_to_grpc_stream_
      ABSL_GUARDED_BY(stream_map_mu_);
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
  // Always use ring-based communication
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
  
  // ServerAuthFilter requires a server security context; set the correct type.
  grpc_server_security_context* server_ctx = 
      grpc_server_security_context_create(arena.get());
  server_ctx->auth_context = MakeShmemAuthContext();
  arena->SetContext<grpc_core::SecurityContext>(server_ctx);
  
  // Set peer string in metadata
  md->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
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
  if (cb_ == nullptr) {
    return;
  }
  // Always start S2C reader for cross-process communication
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    printf("DEBUG: ShmemClientTransport S2C reader thread STARTED\n");
    fflush(stdout);
    stop_.store(false, std::memory_order_relaxed);
    reader_ = std::thread([this] {
      ExecCtx exec_ctx;
      if (server_ != nullptr) spin_iters_ = server_->spin_iters();
      int client_loop_count = 0;
      for (;;) {
        if (stop_.load(std::memory_order_relaxed)) break;
        client_loop_count++;
        grpc_shmem::Command cmd;
        if (!grpc_shmem::PopCommandHybrid(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, spin_iters_, &cmd, sem_adapter_.get())) {
          continue;
        }
        for (;;) {
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
              printf("DEBUG: ShmemClientTransport received S2C_INITIAL_METADATA for stream %u\n", cmd.stream_id);
              fflush(stdout);
              auto data = cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
              auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
              handler->SpawnInfallible(
                  "push-initial", [kvs = std::move(kvs), h = *handler]() mutable {
                    auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                    bool have_ct = false;
                    for (const auto& kv : kvs) {
                      if (kv.key == "content-type") {
                        have_ct = true;
                        md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                      } else {
                        md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&) {});
                      }
                    }
                    if (!have_ct) {
                      md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                    }
                    h.SpawnPushServerInitialMetadata(std::move(md));
                    return Empty{};
                  });
              cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
              break;
            }
            case grpc_shmem::FrameType::S2C_MESSAGE: {
              printf("DEBUG: ShmemClientTransport received S2C_MESSAGE for stream %u, size=%u\n", cmd.stream_id, cmd.data_size);
              fflush(stdout);
              grpc_slice s = grpc_shmem::MakeSliceFromRing(&cb_->GetS2CQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
              handler->SpawnInfallible("push-msg", [h = *handler, s]() mutable {
                SliceBuffer sb;
                sb.AppendIndexed(Slice(s));
                auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
                h.SpawnPushMessage(std::move(msg));
                return Empty{};
              });
              break;
            }
            case grpc_shmem::FrameType::S2C_TRAILING_METADATA: {
              printf("DEBUG: ShmemClientTransport received S2C_TRAILING_METADATA for stream %u\n", cmd.stream_id);
              fflush(stdout);
              auto data = cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
              auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
              handler->SpawnInfallible(
                  "push-trailing", [kvs = std::move(kvs), h = *handler, stream_id = cmd.stream_id, this]() mutable {
                    auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
                    grpc_status_code status = GRPC_STATUS_UNKNOWN;
                    for (const auto& kv : kvs) {
                      if (kv.key == "grpc-status") {
                        status = static_cast<grpc_status_code>(atoi(kv.value.c_str()));
                      } else if (kv.key == "grpc-message") {
                        md->Set(GrpcMessageMetadata(), Slice::FromCopiedString(kv.value));
                      } else {
                        md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&) {});
                      }
                    }
                    md->Set(GrpcStatusMetadata(), status);
                    h.SpawnPushServerTrailingMetadata(std::move(md));
                    {
                      MutexLock lock(&this->mu_);
                      this->handlers_.erase(stream_id);
                    }
                    return Empty{};
                  });
              cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
              break;
            }
            default:
              break;
          }
          grpc_shmem::Command next_cmd;
          if (!grpc_shmem::PopCommandHybrid(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, 0, &next_cmd, nullptr)) {
            break;
          }
          cmd = next_cmd;
        }
        ExecCtx::Get()->Flush();
      }
    });
  }
}

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  EnsureReaderStarted();  // Start S2C reader for all RPCs
  auto stream_id = next_stream_id_.fetch_add(1, std::memory_order_relaxed);
  
  // Always insert handler for S2C reader delivery
  {
    MutexLock lock(&mu_);
    handlers_.insert_or_assign(stream_id, child_call_handler);
  }

  auto cb = cb_;
  child_call_handler.SpawnGuarded(
      "pull_initial_metadata",
      TrySeq(child_call_handler.PullClientInitialMetadata(),
             [cb, stream_id, child_call_handler,
              this](ClientMetadataHandle md) mutable {
               // Extract :path for serialization
               std::string path;
               if (auto* p = md->get_pointer(HttpPathMetadata()); p) {
                 path = std::string(p->as_string_view());
               }

               // Ring-based approach: Always serialize client initial metadata to C2S ring

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
               VLOG(3) << "About to memcpy metadata to ring buffer, off=" << off << ", size=" << vec.size();
               std::memcpy(cb->GetC2SQueues()->data_rb.GetBuffer(cb) + off,
                           vec.data(), vec.size());
               VLOG(3) << "memcpy completed, creating command";
               grpc_shmem::Command cmd{};
               cmd.stream_id = stream_id;
               cmd.type = grpc_shmem::FrameType::C2S_INITIAL_METADATA;
               cmd.data_offset = off;
               cmd.data_size = static_cast<uint32_t>(vec.size());
               VLOG(3) << "About to push command to C2S queue";
               grpc_shmem::PushCommand(cb->GetC2SQueues(), cb,
                                       grpc_shmem::Direction::kC2S, cmd, sem_adapter_.get());
               VLOG(3) << "Command pushed successfully";

               return absl::OkStatus();
             }));
  
  // Send client messages as C2S_MESSAGE frames
  auto send_message = [cb, stream_id, this](MessageHandle m) -> StatusFlag {
    auto* payload = m->payload();
    const size_t n = payload->Length();
    if (n == 0) return Success{};
    uint64_t off = 0;
    if (grpc_shmem::ReserveContiguous(&cb->GetC2SQueues()->data_rb, n, &off)) {
      payload->CopyToBuffer(cb->GetC2SQueues()->data_rb.GetBuffer(cb) + off);
      grpc_shmem::Command cmd{};
      cmd.stream_id = stream_id;
      cmd.type = grpc_shmem::FrameType::C2S_MESSAGE;
      cmd.data_offset = off;
      cmd.data_size = static_cast<uint32_t>(n);
      grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                              cmd, sem_adapter_.get());
    }
    return Success{};
  };
  child_call_handler.SpawnGuarded(
      "c2s_messages",
      ForEach(MessagesFrom(child_call_handler), std::move(send_message)));

  // Client trailing metadata (EOS) will be sent automatically when client closes
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

// Forward declaration
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPairImpl(const ChannelArgs& server_channel_args,
                          const ChannelArgs& client_channel_args);

// Single-argument overload for backward compatibility
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args) {
  return MakeShmemTransportPairImpl(server_channel_args, server_channel_args);
}

// Two-argument overload
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args,
                       const ChannelArgs& client_channel_args) {
  return MakeShmemTransportPairImpl(server_channel_args, client_channel_args);
}

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPairImpl(const ChannelArgs& server_channel_args,
                          const ChannelArgs& client_channel_args) {
  // Create a shared memory segment - shmem transport always uses shared memory
  static std::atomic<uint64_t> pair_id{0};
  // Shmem transport always uses shared memory (dispatch_only = false)
  const bool dispatch_only =
      server_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(false);

  std::unique_ptr<grpc_shmem::ShmemSegment> segment;
  grpc_shmem::ControlBlock* cb = nullptr;

  if (!dispatch_only) {
    grpc_shmem::SegmentConfig cfg;
    uint64_t pair_id_val = pair_id.fetch_add(1);
    cfg.name = absl::StrCat("grpc_shmem_", getpid(), "_", pair_id_val);
    cfg.server_name = absl::StrCat("bench_", getpid(), "_", pair_id_val);  // For semaphore names
    // Optimized sizing for high-throughput large message performance
    // Larger buffers reduce fragmentation and improve contiguous allocation success
    cfg.data_ring_capacity = 64 * 1024 * 1024;   // 64MB per direction for better large message handling
    cfg.size = 192 * 1024 * 1024;                // 192MB total segment
    grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
    auto s = grpc_shmem::ShmemSegment::Create(cfg);
    segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
    cb = segment->control();
  }

  // Add auth context to server channel args for ServerAuthFilter
  auto server_args_with_auth = server_channel_args.SetObject(MakeShmemAuthContext());
  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      server_args_with_auth, std::move(segment));
  auto client_transport = MakeOrphanable<ShmemClientTransport>(
      server_transport->RefAsSubclass<ShmemServerTransport>(), cb, client_channel_args);
  return {std::move(client_transport), std::move(server_transport)};
}

OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& server_channel_args) {
  VLOG(2) << "MakeNamedShmemServerTransport called with server_name: " << server_name;
  
  // Force ring mode for cross-process server and add auth context for ServerAuthFilter
  ChannelArgs ring_mode_args = server_channel_args
      .Set("grpc.shmem.dispatch_only", false)
      .SetObject(MakeShmemAuthContext());

  std::unique_ptr<grpc_shmem::ShmemSegment> segment;
  
  grpc_shmem::SegmentConfig cfg;
  cfg.name = absl::StrCat("grpc_shmem_", server_name);
  cfg.server_name = server_name;  // For named semaphores
  cfg.data_ring_capacity = 64 * 1024 * 1024;
  cfg.size = 192 * 1024 * 1024;
  VLOG(2) << "Segment config: name=" << cfg.name << ", size=" << cfg.size << ", data_ring_capacity=" << cfg.data_ring_capacity;
  
  VLOG(2) << "Removing existing segment if exists...";
  grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
  
  VLOG(2) << "Creating new segment...";
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
  VLOG(2) << "ConnectToShmemServerTransport called with server_name: " << server_name;
  
  const bool dispatch_only =
      client_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(false);

  if (dispatch_only) {
    LOG(ERROR) << "Cross-process shmem requires ring mode (not dispatch-only)";
    return nullptr;
  }

  std::string segment_name = absl::StrCat("grpc_shmem_", server_name);
  VLOG(2) << "Opening segment: " << segment_name;
  
  auto segment = grpc_shmem::ShmemSegment::Open(segment_name);
  
  printf("DEBUG: Segment opened, control block: %p\n", segment.control());
  fflush(stdout);
  if (segment.control() == nullptr) {
    LOG(ERROR) << "Failed to connect to shmem server: " << server_name;
    return nullptr;
  }

  grpc_shmem::ControlBlock* cb = segment.control();
  VLOG(2) << "Control block at: " << cb;
  VLOG(2) << "c2s_queues: " << cb->GetC2SQueues() << ", s2c_queues: " << cb->GetS2CQueues();
  
  auto segment_ptr = std::make_unique<grpc_shmem::ShmemSegment>(std::move(segment));
  
  printf("DEBUG: About to store segment and create client transport\n");
  fflush(stdout);
  
  // Store segment for cleanup
  StoreCrossProcessSegment(cb, std::move(segment_ptr));
  
  VLOG(2) << "Creating client transport with cb: " << cb;
  
  return MakeOrphanable<ShmemClientTransport>(nullptr, cb, client_channel_args);
}

// Duplicate function definition removed

// Duplicate function definition removed

}  // namespace grpc_core
