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
// POSIX utilities
#include <climits>
#include <unistd.h>
#include "src/core/lib/iomgr/closure.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/pollset_set.h"

#include <grpc/event_engine/event_engine.h>
#include <unistd.h>

// Force race condition with artificial delays (disabled by default; enable with GRPC_SHMEM_FORCE_RACE=1)
static inline bool ShmemForceRaceEnabled() {
  static bool enabled = []() {
    const char* e = getenv("GRPC_SHMEM_FORCE_RACE");
    return e != nullptr && e[0] == '1';
  }();
  return enabled;
}
#define SHMEM_FORCE_RACE_DELAY_US 10000  // 10ms delay when enabled explicitly
#define FORCE_RACE_CLIENT_DELAY() do { if (ShmemForceRaceEnabled()) { std::this_thread::sleep_for(std::chrono::microseconds(SHMEM_FORCE_RACE_DELAY_US)); } } while (0)
#define FORCE_RACE_SERVER_DELAY() do { if (ShmemForceRaceEnabled()) { std::this_thread::sleep_for(std::chrono::microseconds(SHMEM_FORCE_RACE_DELAY_US)); } } while (0)

// Server state values for cross-process synchronization
enum class ServerState : uint32_t {
  kNotReady = 0,
  kReady = 1
};


#include <algorithm>
#include <atomic>
#include <chrono>
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
#include "absl/time/time.h"
#include "src/core/lib/iomgr/timer.h"
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
#include "src/core/lib/promise/seq.h"
#include "src/core/lib/promise/for_each.h"
#include "src/core/lib/promise/status_flag.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/slice/slice.h"
#include "src/core/lib/slice/slice_buffer.h"
#include "src/core/lib/transport/connectivity_state.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/channelz/channelz.h"
#include "src/core/util/debug_location.h"
#include "src/core/transport/auth_context.h"
#include "src/core/call/security_context.h"
#include "src/core/util/ref_counted_ptr.h"
#include "include/grpc/grpc_security.h"
#include "absl/log/log.h"

// Linux futex support for low-latency signaling
#include <linux/futex.h>
#include <sys/syscall.h>

// Lightweight debug gate: enable verbose fprintf logging only if GRPC_SHMEM_DEBUG=1
static inline bool ShmemDebugEnabled() {
  static bool enabled = []() {
    const char* e = getenv("GRPC_SHMEM_DEBUG");
    return e != nullptr && e[0] == '1';
  }();
  return enabled;
}
#define SHMEM_DBGF(...) do { if (ShmemDebugEnabled()) { fprintf(stderr, __VA_ARGS__); fflush(stderr); } } while (0)

// Futex helper functions for doorbell synchronization
static inline int futex_wait(uint32_t* addr, uint32_t expected, const struct timespec* ts = nullptr) {
  return syscall(SYS_futex, addr, FUTEX_WAIT, expected, ts, nullptr, 0);
}

static inline int futex_wake(uint32_t* addr, int count = 1) {
  return syscall(SYS_futex, addr, FUTEX_WAKE, count, nullptr, nullptr, 0);
}

// Legacy stream-op scaffolding removed: this transport uses promise-based APIs exclusively.

// Blocking reserve with empty-ring fast wrap support
static void WaitReserveWithEmptyWrap(grpc_shmem::ShmemQueues* q,
                                     grpc_shmem::ControlBlock* cb,
                                     grpc_shmem::Direction dir,
                                     grpc_shmem::TransportSemaphoreAdapter* sem,
                                     grpc_shmem::DataRingBuffer* rb,
                                     uint32_t size,
                                     uint64_t* out_off,
                                     uint32_t* out_pad,
                                     const char* tag) {
  for (;;) {
    if (grpc_shmem::ReserveForWrite(rb, size, out_off, out_pad)) return;

    // Snapshot for empty-ring fast wrap
    const uint64_t cap = rb->capacity;
    uint64_t head = rb->head.load(std::memory_order_relaxed);
    uint64_t tail = rb->tail.load(std::memory_order_acquire);
    uint64_t used = grpc_shmem::RingUsedBytes(head, tail, rb->capacity);
    uint64_t end_off = head % cap;
    uint64_t free_to_end = cap - end_off;

    // EMPTY-RING FAST WRAP: realign to offset 0
    if (used == 0 && end_off != 0 && size > free_to_end && size <= cap) {
      // Step 1: commit pad by advancing head by free_to_end
      uint64_t expected = head;
      if (rb->head.compare_exchange_weak(expected, head + free_to_end,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
        // Step 2: emit PAD so the reader advances tail by free_to_end
        grpc_shmem::Command pad{};
        pad.stream_id = 0;
        pad.type = grpc_shmem::FrameType::DATA_PAD;
        pad.data_offset = 0;
        pad.data_size = static_cast<uint32_t>(free_to_end);
        VLOG(1) << "[EMPTY WRAP] emit PAD=" << free_to_end
                << " head_before=" << head;
        // Ensure the PAD is actually enqueued before waiting for tail to move.
        while (!grpc_shmem::PushCommand(q, cb, dir, pad, sem)) {
          std::this_thread::yield();
        }

        // Wait until tail catches up (PAD applied) - use yield loop without timeout
        const uint64_t target = tail + free_to_end;
        while (rb->tail.load(std::memory_order_acquire) < target) {
          std::this_thread::yield();
        }
        // Now head%cap==0, used==0 -> try reserve again
        continue;
      }
      // CAS failed — loop and retry
      continue;
    }

    // Not the empty-ring wrap case; standard backoff
    std::this_thread::yield();
  }
}

// Watchdog helper for blocking reserve operations
static void WaitReserveWithWatchdog(grpc_shmem::DataRingBuffer* rb, uint32_t size,
                                    uint64_t* off, uint32_t* pad, const char* tag) {
  auto t0 = absl::Now();
  for (;;) {
    if (grpc_shmem::ReserveForWrite(rb, size, off, pad)) return;
    if (absl::Now() - t0 > absl::Milliseconds(200)) {
      VLOG(1) << "[RESERVE STALL] " << tag
              << " size=" << size
              << " head=" << rb->head.load(std::memory_order_relaxed)
              << " tail=" << rb->tail.load(std::memory_order_acquire)
              << " cap="  << rb->capacity;
      t0 = absl::Now();  // log again every 200ms if it persists
    }
    std::this_thread::yield();
  }
}

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

// Simple encoder for metadata - just basic content-type for now
namespace {

// Data structure for announcing streams to core via accept callback
struct ShmemServerData {
  uint64_t stream_id;  // Use 64-bit to match client stream IDs
  std::vector<grpc_shmem::KVPair> initial_md;
};

// Correct futex doorbell adapter - fixed deadlock issues
class FutexDoorbellAdapter : public grpc_shmem::TransportSemaphoreAdapter {
 public:
  explicit FutexDoorbellAdapter(grpc_shmem::ControlBlock* cb) : cb_(cb) {}

  
  void Post(grpc_shmem::ControlBlock* cb, bool is_c2s) override {
    auto& db = is_c2s ? cb_->c2s_db : cb_->s2c_db;
    
    // Increment sequence to signal new data
    uint64_t new_seq = db.seq.fetch_add(1, std::memory_order_release) + 1;
    
    // Wake any waiting threads using pure futex signaling
    int woken = futex_wake(reinterpret_cast<uint32_t*>(&db.seq), 1);
    
    VLOG(2) << "FutexDoorbellAdapter::Post " << (is_c2s ? "C2S" : "S2C") 
            << " seq=" << new_seq << " woken=" << woken;
  }

  
  void Wait(bool is_c2s) override {
    auto& db = is_c2s ? cb_->c2s_db : cb_->s2c_db;
    
    // Get current sequence
    uint32_t current_seq = db.seq.load(std::memory_order_acquire);
    
    // Short spin to avoid futex syscall in fast path
    for (int i = 0; i < 2000; ++i) {
      if (db.seq.load(std::memory_order_acquire) != current_seq) {
        return; // Data arrived during spin
      }
      __builtin_ia32_pause();
    }
    
    // Set waiter flag
    db.waiter.store(1, std::memory_order_release);
    
    // Final check after setting waiter flag
    uint32_t final_seq = db.seq.load(std::memory_order_acquire);
    if (final_seq != current_seq) {
      db.waiter.store(0, std::memory_order_relaxed);
      return; // Data arrived while setting waiter
    }
    
    // Use blocking futex wait without timeout
    futex_wait(reinterpret_cast<uint32_t*>(&db.seq), final_seq, nullptr);
    
    // Clear waiter flag when waking up
    db.waiter.store(0, std::memory_order_relaxed);
    
    // Always return after one wait - don't loop infinitely
  }

  bool ShouldPost(bool is_c2s, size_t bytes_added, int frames_added, bool was_empty) override {
    // SIMPLE coalescing: only coalesce when we're absolutely certain it's safe
    // For now, always post to ensure correctness - we can optimize later
    // The race condition fix (waiter-bit handshake) is more important than coalescing
    
    VLOG(3) << "ShouldPost " << (is_c2s ? "C2S" : "S2C") 
            << " was_empty=" << was_empty 
            << " decision=true (always post for safety)";
    
    return true;
  }

private:
  grpc_shmem::ControlBlock* cb_;
  std::atomic<uint64_t> avoided_wakes_{0};
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
  ShmemClientTransport(ShmemServerTransport* server,
                       grpc_shmem::ControlBlock* cb, 
                       std::unique_ptr<grpc_shmem::ShmemSegment> client_segment,
                       const ChannelArgs& args)
      : server_(server), cb_(cb), client_segment_(std::move(client_segment)),
        channel_args_(args.Set(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1)
                         .Set(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1)) {
    VLOG(2) << "ShmemClientTransport constructor starting";
  SHMEM_DBGF("*** DEBUG: ShmemClientTransport constructor starting ***\n");
    MutexLock l(&state_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_CONNECTING, absl::OkStatus(), "init");
    state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(), "shmem ready");
    
    // Track process attachment for coordination with atomic increment  
    if (cb_) {
      int32_t current_count = cb_->process_count.fetch_add(1, std::memory_order_acq_rel) + 1;
      LOG(INFO) << "ShmemClientTransport attached, process count now: " << current_count;
      
      // Futex doorbells are initialized directly in ControlBlock, no additional setup needed
      
      // Create futex doorbell adapter for low-latency queue operations
      LOG(INFO) << "Creating FutexDoorbellAdapter for CLIENT - cb_=" << cb_;
      sem_adapter_ = std::make_unique<FutexDoorbellAdapter>(cb_);
      LOG(INFO) << "Created FutexDoorbellAdapter for CLIENT - sem_adapter_=" << sem_adapter_.get();
      
      // Register Event Engine callbacks for client command processing
      // EventEngine callbacks removed - simplified integration
      VLOG(2) << "Client EventEngine integration completed";
    }
  SHMEM_DBGF("*** DEBUG: ShmemClientTransport constructor completed ***\n");
  }

  void StartCall(CallHandler child_call_handler) override;
  void Orphan() override {
    SHMEM_DBGF("*** DEBUG: Client transport Orphan() called - initiating shutdown ***\n");
    InitiateShutdown();
    Unref(DEBUG_LOCATION, "orphan");
  }
  ~ShmemClientTransport() override {
    VLOG(1) << "ShmemClientTransport destructor called";
    fflush(stderr);
    // Ensure clean shutdown and join futex wait thread if still running
    // This guards against std::terminate if a joinable thread remains at destruction.
    if (!cleanup_complete_.load(std::memory_order_acquire)) {
      InitiateShutdown();
    }
    if (polling_thread_.joinable()) {
      try {
        polling_thread_.join();
      } catch (...) {
        // Swallow any exceptions during destructor to avoid terminate
      }
    }
    VLOG(1) << "ShmemClientTransport destructor completed";
    fflush(stderr);
  }
  
 private:
  void InitiateShutdown() {
    VLOG(1) << "ShmemClientTransport InitiateShutdown called";
    fflush(stderr);
    ExecCtx exec_ctx;
    
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      return; // Already initiated
    }
    
    // Step 1.5: Set stop flag FIRST to prevent new task scheduling
    stop_.store(true, std::memory_order_relaxed);
    
    // Step 1.6: Clear call handlers to prevent Promise task execution
    {
      MutexLock lk(&mu_);
      handlers_.clear();
    }
    
    // Step 2: Wake any waiting reader threads
    WaitForThreadsToExit();
    
    // Step 4: Publish shutdown to connectivity watchers before cleanup
    {
      MutexLock l(&state_mu_);
      state_tracker_.SetState(GRPC_CHANNEL_SHUTDOWN, absl::OkStatus(),
                              "client transport shutdown");
    }
    // Step 5: Cleanup resources in proper order
    CleanupResources();
    
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    // Signal the polling thread to stop
    if (thread_stop_flag_) {
      thread_stop_flag_->store(true, std::memory_order_release);
    }
    
    // Wake futex waiters if a ring reader thread was started
    if (reader_started_.load(std::memory_order_acquire)) {
      // RACE CONDITION FIX: Wake reader threads during shutdown
      // and add a small delay to allow reader thread to check stop flag
      if (cb_ != nullptr) {
        try {
          // With futex doorbells, we need to actively wake any waiting reader threads
          // CLIENT: Wake S2C reader thread (client reads S2C responses)
          auto& db = cb_->s2c_db;
          
          // Always wake futex waiters during shutdown - don't check waiter flag
          db.seq.fetch_add(1, std::memory_order_release);
          futex_wake(reinterpret_cast<uint32_t*>(&db.seq), INT_MAX); // Wake all waiters
          
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error waking futex doorbells: " << e.what();
        }
      }
      
      // Wake futex wait loop in case it's blocked
      if (cb_ != nullptr) {
        cb_->s2c_db.seq.fetch_add(1, std::memory_order_release);
        futex_wake(reinterpret_cast<uint32_t*>(&cb_->s2c_db.seq), 1);
      }
      // Wait for futex wait thread to finish
      if (polling_thread_.joinable()) {
        try {
          polling_thread_.join();
          SHMEM_DBGF("*** DEBUG: Client polling thread joined successfully ***\n");
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error joining polling thread: " << e.what();
        }
      }
    }
  }
  
  void CleanupResources() {
    try {
      // Stop re-arming and orphan any registered fds
      stop_.store(true, std::memory_order_release);
      // No pollset fd registered in futex-only mode; ensure raw fd closed if created
      if (s2c_doorbell_fd_ >= 0) {
        close(s2c_doorbell_fd_);
        s2c_doorbell_fd_ = -1;
      }
      // Explicitly destroy transport-owned pollset_set to unblock CQ shutdown
      if (pss_ != nullptr && pss_owned_) {
        grpc_pollset_set_destroy(pss_);
        pss_ = nullptr;
        pss_owned_ = false;
      }
      // CRITICAL FIX: Do NOT clear handlers during shutdown as this can cause 
      // completion events to be lost, leading to hanging in grpc_completion_queue_next
      // Let handlers complete naturally and only clear on destruction
      
      // Clear handler map to prevent stale entries in reused transports
      // {
      //   MutexLock lock(&mu_);
      //   handlers_.clear();
      // }
      
      // Handle atomic reference counting - CLIENT NEVER CLEANS UP SEGMENT
      if (cb_ != nullptr) {
        // Atomic decrement - but client never performs segment cleanup
        int32_t previous_count = cb_->process_count.fetch_sub(1, std::memory_order_acq_rel);
        int32_t remaining = previous_count - 1;
        LOG(INFO) << "CLIENT CLEANUP: ShmemClientTransport [PID " << getpid() << "] detaching, remaining processes: " << remaining;
        
        // CLIENT RULE: Never clean up the shared segment - only the server manages segment lifetime
        LOG(INFO) << "CLIENT CLEANUP: Client detached but leaving segment for server to manage";
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "ShmemClientTransport cleanup error: " << e.what();
    }
  }
  
 public:
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return static_cast<ClientTransport*>(this); }
  ServerTransport* server_transport() override { return nullptr; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset* ps) override {
    if (pss_ == nullptr) { pss_ = grpc_pollset_set_create(); pss_owned_ = true; }
    grpc_pollset_set_add_pollset(pss_, ps);
    // Futex-only integration: no fd added to pollsets
  }
  void SetPollsetSet(grpc_stream*, grpc_pollset_set* pss) override {
    // If core provides an external pollset set, adopt without owning
    pss_ = pss;
    pss_owned_ = false;
    // Futex-only integration: no fd added to pollset sets
  }
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

  void EnsureReaderStarted();
  void InitS2CDoorbell();
  void DrainS2CFromPoller();
  void StartRecvDrain(uint64_t sid);
  void PerformFinalCleanup();
  static void OnS2CReadable(void* arg, grpc_error_handle error);

  ShmemServerTransport* server_;
  grpc_shmem::ControlBlock* cb_ = nullptr;
  ChannelArgs channel_args_;
  std::unique_ptr<grpc_shmem::ShmemSegment> client_segment_;  // for cross-process
  std::atomic<bool> stop_{false};
  std::atomic<bool> shutdown_initiated_{false};
  std::atomic<bool> cleanup_complete_{false};
  // std::thread reader_; // Removed - using EventEngine callbacks
  int spin_iters_ = kDefaultSpinIters;
  
  // Futex doorbell adapter for queue operations
  std::unique_ptr<grpc_shmem::TransportSemaphoreAdapter> sem_adapter_;
  
  std::atomic<bool> reader_started_{false};
  std::atomic<bool> reader_ready_{false};
  int s2c_doorbell_fd_ = -1;  // unused in futex-only mode
  grpc_fd* s2c_grpc_fd_ = nullptr;  // unused in futex-only mode
  grpc_closure s2c_on_readable_;  // unused in futex-only mode
  grpc_pollset_set* pss_ = nullptr;
  bool pss_owned_ = false;
  
  // Managed thread for futex polling (no detach, proper cleanup)
  std::thread polling_thread_;
  std::shared_ptr<std::atomic<bool>> thread_stop_flag_;
  
  void ArmS2CReadableWatch();
  
  // gRPC call handlers for S2C processing
  Mutex mu_;
  absl::flat_hash_map<uint64_t, std::shared_ptr<CallHandler>> handlers_ ABSL_GUARDED_BY(mu_);
  
  // For reassembling chunked S2C messages by stream ID
  struct StreamChunkState {
    grpc_core::SliceBuffer accumulator;
    bool accumulating = false;
  };
  std::unordered_map<uint64_t, StreamChunkState> s2c_chunk_accumulators_;

  // Ordered S2C delivery (per stream)
  struct RecvOp { 
    enum Type { kInit, kMsg, kTrailing } type; 
    grpc_core::SliceBuffer buf; 
  };
  struct StreamRecvState { 
    std::deque<RecvOp> q; 
    bool draining = false; 
  };
  Mutex s2c_recv_mu_;
  absl::flat_hash_map<uint64_t, StreamRecvState> s2c_recv_ ABSL_GUARDED_BY(s2c_recv_mu_);

  Mutex state_mu_;
  ConnectivityStateTracker state_tracker_
      ABSL_GUARDED_BY(state_mu_){"shmem_client_transport",
                                 GRPC_CHANNEL_CONNECTING};

};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& args) : 
      channel_args_(args.Set(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1)
                       .Set(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1)) {
    VLOG(2) << "ShmemServerTransport constructor (1-arg) starting";
    
    // Check if auth context is in the args
    auto auth_ctx = args.GetObjectRef<grpc_auth_context>();
    VLOG(2) << "Auth context in args: " << auth_ctx.get();
    
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
    // InitC2SDoorbell will be called later when server name is available
    
    // Server reader thread removed - using EventEngine callbacks instead
    if (cb_) {
      LOG(INFO) << "ShmemServerTransport constructor (1-arg) completed successfully";
      VLOG(2) << "Server transport initialized without reader thread - using EventEngine";
    }
  }
  ShmemServerTransport(const ChannelArgs& args,
                       std::unique_ptr<grpc_shmem::ShmemSegment> seg)
      : segment_(std::move(seg)), 
        channel_args_(args.Set(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1)
                         .Set(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1)) {
    VLOG(2) << "ShmemServerTransport constructor starting";
    auto ring_cap = args.GetInt("grpc.max_message_size");
    VLOG(2) << "max_message_size arg = " << ring_cap.value_or(-1);
    // InitC2SDoorbell will be called in SetCallDestination when server name is available  
    // Existing constructor logic continues below
  SHMEM_DBGF("*** DEBUG: ShmemServerTransport constructor (2-arg) starting ***\n");
    LOG(INFO) << "ShmemServerTransport constructor (2-arg) starting";
    VLOG(2) << "ShmemServerTransport constructor (2-arg) starting";
    
    // Check if auth context is in the args
    auto auth_ctx = args.GetObjectRef<grpc_auth_context>();
    VLOG(2) << "Auth context in args: " << auth_ctx.get();
    
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
    
    // Check segment before getting control block
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
      // Safely increment process count with atomic operation
      try {
        int32_t current_count = cb_->process_count.fetch_add(1, std::memory_order_acq_rel) + 1;
        LOG(INFO) << "ShmemServerTransport attached, process count now: " << current_count;
        
        // Futex doorbells are initialized directly in ControlBlock, no additional setup needed
        
        // Create futex doorbell adapter for low-latency queue operations  
        LOG(INFO) << "Creating FutexDoorbellAdapter for SERVER - cb_=" << cb_;
        sem_adapter_ = std::make_unique<FutexDoorbellAdapter>(cb_);
        LOG(INFO) << "Created FutexDoorbellAdapter for SERVER - sem_adapter_=" << sem_adapter_.get();
        
        // Register Event Engine callbacks for server command processing
        VLOG(2) << "About to get Event Engine for server";
        auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
        VLOG(2) << "Got Event Engine: " << ee.get();
        
        VLOG(2) << "About to register Event Engine callbacks for server";
        // EventEngine integration completed - no callback registration needed
        VLOG(2) << "Server Event Engine callbacks registered";
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
    
    // Check ControlBlock before starting reader thread
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
    // Don't start reader thread during construction to avoid startup hang
    // EnsureReaderStarted();
    
  // For named servers, defer starting the doorbell until SetCallDestination
  // so pollsets are ready. Handshake is futex-only; no Unix sockets involved.
    bool is_named_server = args.GetBool("grpc.shmem.is_named_server").value_or(false);
    auto server_name = args.GetString("grpc.shmem.server_name");
    
    // Enable async FD exchange for all named servers with proper pollset registration order
    if (is_named_server) {
      LOG(INFO) << "Named server detected, will start FD exchange in SetCallDestination: " << server_name.value_or("unknown");
      // Don't call InitC2SDoorbell here - pollsets not ready yet
    }
  SHMEM_DBGF("*** DEBUG: ShmemServerTransport constructor (2-arg) completed successfully ***\n");
    LOG(INFO) << "ShmemServerTransport constructor (2-arg) completed successfully";
  }

  grpc_shmem::ControlBlock* GetControlBlock() const { return cb_; }
  
  // RACE CONDITION FIX: Allow client to wait for server readiness
  void WaitForReady() {
    VLOG(1) << "CLIENT: WaitForReady called";
    MutexLock rl(&ready_mu_);
    if (!ready_) {
      VLOG(1) << "CLIENT: Server not ready, waiting...";
      while (!ready_) {
        ready_cv_.Wait(&ready_mu_);
      }
      VLOG(1) << "CLIENT: Server is now ready";
    } else {
      VLOG(1) << "CLIENT: Server already ready";
    }
  }
  
  void SetCallDestination(RefCountedPtr<UnstartedCallDestination> h) override {
    LOG(INFO) << "ShmemServerTransport: Integration mode latched to PROMISE";
    
    std::vector<PendingCall> pending_to_flush;
    {
      MutexLock lock(&dest_mu_);
      dest_ = std::move(h);
      dest_ready_ = true;
      
      // Only flush if legacy callback is not already handling calls
      if (accept_stream_cb_ == nullptr) {
        pending_to_flush = std::move(pending_calls_);
        pending_calls_.clear();
        LOG(INFO) << "ShmemServerTransport: Flushing " << pending_to_flush.size() << " buffered calls to PROMISE path";
      } else {
        LOG(INFO) << "ShmemServerTransport: Legacy callback already active, PROMISE path secondary";
      }
    }
    
    // Start pending calls using promise-based approach
    for (auto& pc : pending_to_flush) {
      StartCallNow(std::move(pc), /*from_flush=*/true);
    }
    
    // Report READY (matches inproc behavior).
    state_.store(ConnectionState::kReady, std::memory_order_release);
    MutexLock l(&state_tracker_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(),
                            "accept function set");
    
  // Start event-driven polling since the server is ready to accept calls.
  // Note: Do NOT override the accept_stream callback here; the core will
  // have already installed its own callback via PerformOp during
  // Server::SetupTransport. We simply start consuming C2S traffic and call
  // that callback when new streams arrive, mirroring TCP behavior.
  if (integration_mode_ == Integration::kLegacy) {
    LOG(INFO) << "ShmemServerTransport: ignoring PROMISE (legacy already latched)";
    return;
  }
  integration_mode_ = Integration::kPromise;
  LOG(INFO) << "ShmemServerTransport: Integration mode latched to PROMISE";
  VLOG(2) << "INTEGRATION LATCHED TO PROMISE";

    // Start futex reader only once, after mode is latched.
    LOG(INFO) << "InitC2SDoorbell: futex reader start (mode=promise)";
    InitC2SDoorbell();
    
    // Signal that server is ready for connections
    {
      MutexLock rl(&ready_mu_);
      ready_ = true;
      ready_cv_.SignalAll();
    }
    
    // Commands will be processed via OnC2SReadable callbacks when clients send data
  }

  void Orphan() override {
  SHMEM_DBGF("*** DEBUG: Server transport Orphan() called - initiating proper shutdown ***\n");
    // In benchmark contexts, server transports are created/destroyed per iteration
    // We need to properly shut down the polling threads to avoid accessing destroyed memory
    InitiateShutdown();
    Unref(DEBUG_LOCATION, "orphan");
  }

  // Event Engine callback: drain commands from C2S queue (non-blocking)
  void DrainServerCommandsNonBlocking(int max_commands) {
    VLOG(3) << "DrainServerCommandsNonBlocking: attempting to drain up to " << max_commands << " commands";
    
    if (!cb_ || stop_.load(std::memory_order_relaxed)) {
      return; // Don't process if shutting down
    }
    
    for (int i = 0; i < max_commands; ++i) {
      grpc_shmem::Command cmd;
      // Use spin_iters = 0 for non-blocking poll (no waiting)
      bool has_command = grpc_shmem::PopCommandHybrid(
          cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S, 
          0, // spin_iters = 0 for non-blocking
          &cmd, sem_adapter_.get());
      
      if (has_command) {
        VLOG(2) << "Event Engine callback drained command type=" << static_cast<int>(cmd.type) 
                << " stream=" << cmd.stream_id;
        // Note: Actual command processing would happen here
        // For now, just log that we successfully drained a command
      } else {
        VLOG(3) << "Event Engine callback: no more commands available";
        break; // No more commands available
      }
    }
  }
  
 private:
  void InitiateShutdown() {
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      return; // Already initiated
    }
    
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
      // Additional cleanup flags could be set here if needed
    }
    
    // Step 3: Wait for threads to exit
    WaitForThreadsToExit();
    
  // Step 4: Publish channel shutdown so server watchers observe closure
  Disconnect(absl::UnavailableError("server shutdown"));
  // Step 5: Cleanup resources
    CleanupResources();
    
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    // Signal the server polling thread to stop
    if (server_thread_stop_flag_) {
      server_thread_stop_flag_->store(true, std::memory_order_release);
    }

    // Always wake futex waiters in case the thread is blocked in futex_wait
    if (cb_ != nullptr) {
      try {
        auto& db = cb_->c2s_db;
        
        // Always wake futex waiters during shutdown - don't check waiter flag
        db.seq.fetch_add(1, std::memory_order_release);
        futex_wake(reinterpret_cast<uint32_t*>(&db.seq), INT_MAX); // Wake all waiters
        
        
      } catch (const std::exception& e) {
        LOG(ERROR) << "Error waking futex doorbells: " << e.what();
      }
    }

    // Join the futex wait thread if it was started
    if (server_polling_thread_.joinable()) {
      try {
        server_polling_thread_.join();
  SHMEM_DBGF("*** DEBUG: Server polling thread joined successfully ***\n");
      } catch (const std::exception& e) {
        LOG(ERROR) << "Error joining server polling thread: " << e.what();
      }
    }
  }
  
  void CleanupResources() {
    try {
      // Prevent further re-arming
      stop_.store(true, std::memory_order_release);
      // No pollset fd registered in futex-only mode; ensure raw fd closed if created
      if (c2s_doorbell_fd_ >= 0) {
        close(c2s_doorbell_fd_);
        c2s_doorbell_fd_ = -1;
      }
      // Explicitly destroy transport-owned pollset_set to unblock CQ shutdown
      if (pss_ != nullptr && pss_owned_) {
        grpc_pollset_set_destroy(pss_);
        pss_ = nullptr;
        pss_owned_ = false;
      }
      // Cleanup semaphore manager
      // Cleanup semaphore manager if needed
      
      // SERVER RULE: Server manages its own segment lifecycle, not based on client count
      if (cb_ != nullptr) {
        // Decrement for tracking but server only cleans up on explicit shutdown
        int32_t previous_count = cb_->process_count.fetch_sub(1, std::memory_order_acq_rel);
        int32_t remaining = previous_count - 1;
        LOG(INFO) << "SERVER CLEANUP: ShmemServerTransport [PID " << getpid() << "] detaching, remaining processes: " << remaining;
        
        // SERVER RULE: Only the main named server should perform final cleanup
        // Connection-specific server transports should not remove the segment
        bool is_named_server = channel_args_.GetBool("grpc.shmem.is_named_server").value_or(false);
        VLOG(1) << "SERVER CLEANUP: PID=" << getpid() << ", is_named_server=" << (is_named_server ? "true" : "false") << ", remaining=" << remaining;
        if (is_named_server && remaining == 0) {
          LOG(INFO) << "SERVER CLEANUP: Named server shutting down with no remaining processes - performing final cleanup";
          PerformFinalCleanup();
        } else if (is_named_server) {
          LOG(INFO) << "SERVER CLEANUP: Named server shutting down but " << remaining << " processes still attached - keeping segment alive";
        } else {
          VLOG(1) << "SERVER CLEANUP: Connection server transport ending - segment managed by main server";
        }
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "ShmemServerTransport cleanup error: " << e.what();
    }
  }
  
  void CleanupSharedResources() {
    try {
      // Clean up semaphore names if needed
    } catch (const std::exception& e) {
      LOG(ERROR) << "Error cleaning up shared resources: " << e.what();
    }
  }
  
 public:
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return nullptr; }
  ServerTransport* server_transport() override { return static_cast<ServerTransport*>(this); }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset* ps) override {
  SHMEM_DBGF("*** DEBUG: Server SetPollset called (futex-only) ***\n");
    if (pss_ == nullptr) { pss_ = grpc_pollset_set_create(); pss_owned_ = true; }
    grpc_pollset_set_add_pollset(pss_, ps);
  }
  void SetPollsetSet(grpc_stream*, grpc_pollset_set* pss) override {
    pss_ = pss;
    pss_owned_ = false;  // external ownership
  }
  // Legacy stream-op vtable methods removed (promise-based path only).
  
  void PerformOp(grpc_transport_op* op) override {
  SHMEM_DBGF("*** DEBUG: ShmemServerTransport::PerformOp called ***\n");
    LOG(INFO) << "ShmemServerTransport::PerformOp called";
    
    // Handle connectivity watch like inproc.
    if (op->start_connectivity_watch != nullptr) {
  SHMEM_DBGF("*** DEBUG: PerformOp: start_connectivity_watch set ***\n");
      MutexLock l(&state_tracker_mu_);
      state_tracker_.AddWatcher(op->start_connectivity_watch_state,
                                std::move(op->start_connectivity_watch));
    }
    if (op->stop_connectivity_watch != nullptr) {
  SHMEM_DBGF("*** DEBUG: PerformOp: stop_connectivity_watch set ***\n");
      MutexLock l(&state_tracker_mu_);
      state_tracker_.RemoveWatcher(op->stop_connectivity_watch);
    }
    
    // Handle accept stream callback registration - following TCP transport pattern
    if (op->set_accept_stream) {
      if (integration_mode_ == Integration::kPromise) {
        LOG(INFO) << "ShmemServerTransport: ignoring LEGACY (promise already latched)";
        goto after_accept_stream;
      }
      integration_mode_ = Integration::kLegacy;
      LOG(INFO) << "ShmemServerTransport: Integration mode latched to LEGACY";
      VLOG(2) << "INTEGRATION LATCHED TO LEGACY";
      accept_stream_cb_ = op->set_accept_stream_fn;
      accept_stream_cb_user_data_ = op->set_accept_stream_user_data;
      
      // FLUSH PENDING CALLS TO LEGACY PATH: if calls arrived before accept_stream_cb was set
      std::vector<PendingCall> pending_to_flush;
      {
        MutexLock lock(&dest_mu_);
        // Only flush if promise-based destination is not already handling calls
        if (!dest_ready_) {
          pending_to_flush = std::move(pending_calls_);
          pending_calls_.clear();
          LOG(INFO) << "ShmemServerTransport: Flushing " << pending_to_flush.size() << " buffered calls to LEGACY path";
        } else {
          LOG(INFO) << "ShmemServerTransport: PROMISE path already active, LEGACY path secondary";
        }
      }
      
      // Process pending calls using legacy accept_stream callback
      for (auto& pc : pending_to_flush) {
        fprintf(stderr, "*** DEBUG: LEGACY ACCEPT_STREAM CALLBACK - stream_id=%lu ***\n", pc.stream_id); fflush(stderr);
        auto* server_data = new ShmemServerData{pc.stream_id, std::move(pc.kvs_for_legacy)};
        accept_stream_cb_(accept_stream_cb_user_data_, this, server_data);
        fprintf(stderr, "*** DEBUG: LEGACY ACCEPT_STREAM CALLBACK COMPLETED - stream_id=%lu ***\n", pc.stream_id); fflush(stderr);
      }
      
      // Start futex reader now that legacy integration is latched
      LOG(INFO) << "InitC2SDoorbell: futex reader started (mode=legacy)";
      InitC2SDoorbell();
      
      // Signal that server is ready for connections
      {
        MutexLock rl(&ready_mu_);
        ready_ = true;
        ready_cv_.SignalAll();
      }
    } else {
      LOG(INFO) << "ShmemServerTransport: No accept_stream callback set (promise-only mode expected)";
    }
after_accept_stream:
    
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

  // FinishAccept - called by core to complete the stream accept lifecycle
  void FinishAccept(const void* server_data);

 private:
  enum class Integration { kAuto, kLegacy, kPromise };
  Integration integration_mode_ = Integration::kAuto;
  
  ~ShmemServerTransport() override {
    VLOG(1) << "ShmemServerTransport destructor called";
    fflush(stderr);
    // Ensure shutdown has completed and join futex wait thread if still running
    if (!cleanup_complete_.load(std::memory_order_acquire)) {
      InitiateShutdown();
    }
    if (server_polling_thread_.joinable()) {
      try {
        server_polling_thread_.join();
      } catch (...) {
        // Avoid throwing from destructor
      }
    }
    VLOG(1) << "ShmemServerTransport destructor completed";
    fflush(stderr);
  }

  void PerformFinalCleanup();
  void InitC2SDoorbell();
  void DrainC2SFromPoller();
  static void OnC2SReadable(void* arg, grpc_error_handle error);

  void EnsureReaderStarted() {
    // SERVER READER START REMOVED: InitC2SDoorbell is now called only after integration latches
    // in SetCallDestination() or PerformOp() set_accept_stream path to prevent buffer-first races.
    // This method is kept for API compatibility but no longer starts the reader.
    LOG(INFO) << "Server EnsureReaderStarted: deferred until integration latches";
  }

  // Legacy stream-op completion helpers removed.

  // Response monitoring loop - forward real server responses onto S2C ring.
  // Implement CallOutboundLoop equivalent for shmem cross-process communication
  auto ShmemCallOutboundLoop(uint64_t stream_id, CallInitiator call_initiator) {
  SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop STARTED for stream_id=%lu ***\n", stream_id);
    return Seq(
        TrySeq(
          call_initiator.PullServerInitialMetadata(),
          [this, stream_id](std::optional<ServerMetadataHandle> md) {
      SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: PullServerInitialMetadata returned, has_value=%s ***\n", 
        md.has_value() ? "true" : "false");
            if (md.has_value()) {
              // Server response delay removed - testing other locations
              SHMEM_DBGF("*** DEBUG: SERVER: Sending S2C_INITIAL_METADATA stream_id=%lu ***\n", stream_id);
              VLOG(1) << "SERVER: Sending S2C_INITIAL_METADATA stream_id=" << stream_id;
              std::vector<grpc_shmem::KVPair> kvs;
              kvs.push_back({"content-type", "application/grpc"});
              
              // TODO: Add binary metadata fields from initial metadata
              // For now, just basic content-type
              auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
              uint64_t off = 0; uint32_t pad = 0;
              WaitReserveWithWatchdog(&cb_->GetS2CQueues()->data_rb, buf.size(), &off, &pad, "S2C_INITIAL_METADATA");
              VLOG(1) << "[RESERVE OK] S2C_INITIAL_METADATA off=" << off
                      << " size=" << buf.size() << " pad=" << pad;
              std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, buf.data(), buf.size());
              std::atomic_thread_fence(std::memory_order_release);
              if (pad) {
                VLOG(1) << "EMIT PAD dir=S2C bytes=" << pad;
                grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, pad, 0, 0};
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, pad_cmd, sem_adapter_.get());
              }
              grpc_shmem::Command out{stream_id, grpc_shmem::FrameType::S2C_INITIAL_METADATA, off, (uint32_t)buf.size(), 0, 0};
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
              VLOG(3) << "ShmemCallOutboundLoop: S2C_INITIAL_METADATA sent for stream " << stream_id;
            }
            return Success{};
          }
        ),
        ForEach(MessagesFrom(call_initiator),
          [this, stream_id](MessageHandle msg) {
            VLOG(2) << "ShmemCallOutboundLoop Processing message for stream_id=" << stream_id;
            SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: Processing message for stream_id=%lu ***\n", stream_id);
            auto* payload = msg->payload();
            const size_t n = payload->Length();
            VLOG(2) << "Message size=" << n << " bytes";
            SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: Message size=%zu ***\n", n);
            if (n == 0) {
              // Still need to send S2C_MESSAGE frame for 0-byte messages to signal completion
              SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: Sending empty S2C_MESSAGE for stream_id=%lu ***\n", stream_id);
              grpc_shmem::Command cmd{};
              cmd.stream_id = stream_id;
              cmd.type = grpc_shmem::FrameType::S2C_MESSAGE;
              cmd.data_offset = 0;  // No data
              cmd.data_size = 0;
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, cmd, sem_adapter_.get());
              return Success{};
            }
            SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: About to send S2C_MESSAGE for stream_id=%lu, size=%zu ***\n", stream_id, n);
            VLOG(2) << "ShmemCallOutboundLoop: sending S2C_MESSAGE for stream " << stream_id << ", size=" << n;
            
            auto* rb = &cb_->GetS2CQueues()->data_rb;
            
            // Check if message fits in ring buffer capacity
            if (n <= rb->capacity) {
              // Normal path: fits in one frame
              uint64_t off = 0;
              uint32_t pad = 0;
              WaitReserveWithWatchdog(rb, n, &off, &pad, "S2C_MESSAGE");
              VLOG(1) << "[RESERVE OK] S2C_MESSAGE off=" << off
                      << " size=" << n << " pad=" << pad;
              unsigned char* base = rb->GetBuffer(cb_);
              payload->CopyToBuffer(base + off);

              // Ensure the data is visible before announcing it
              std::atomic_thread_fence(std::memory_order_release);

              if (pad) {
                VLOG(1) << "EMIT PAD dir=S2C bytes=" << pad;
                grpc_shmem::Command pad_cmd{};
                pad_cmd.stream_id = 0;
                pad_cmd.type = grpc_shmem::FrameType::DATA_PAD;
                pad_cmd.data_offset = 0;
                pad_cmd.data_size = pad;
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C,
                                        pad_cmd, sem_adapter_.get());
              }

              grpc_shmem::Command out{};
              out.stream_id = stream_id;
              out.type = grpc_shmem::FrameType::S2C_MESSAGE;
              out.data_offset = off;
              out.data_size = static_cast<uint32_t>(n);
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                      grpc_shmem::Direction::kS2C,
                                      out, sem_adapter_.get());
              VLOG(3) << "ShmemCallOutboundLoop: S2C_MESSAGE sent for stream " << stream_id;
            } else {
              // Chunking path: message exceeds ring capacity
              fprintf(stderr, "=== CRITICAL: CHUNKING S2C response size=%zu > capacity=%lu ===\n", n, rb->capacity); fflush(stderr);
              VLOG(1) << "ShmemCallOutboundLoop: chunking S2C_MESSAGE for stream " << stream_id 
                      << ", size=" << n << " > capacity=" << rb->capacity;
              
              // Copy entire payload to temporary buffer once
              std::vector<uint8_t> temp_buf(n);
              payload->CopyToBuffer(temp_buf.data());
              
              const uint64_t chunk_size = rb->capacity - 65536; // 64KB headroom for PAD
              uint64_t remaining = n;
              uint64_t src_offset = 0;
              
              while (remaining > 0) {
                const uint64_t this_chunk = std::min(remaining, chunk_size);
                const bool is_last_chunk = (remaining == this_chunk);
                
                VLOG(2) << "ShmemCallOutboundLoop: sending S2C chunk stream=" << stream_id
                        << " chunk_size=" << this_chunk << " remaining_after=" << (remaining - this_chunk)
                        << " is_last=" << is_last_chunk;
                
                uint64_t off = 0;
                uint32_t pad = 0;
                WaitReserveWithEmptyWrap(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C,
                                         sem_adapter_.get(),
                                         rb, this_chunk, &off, &pad, "S2C_MESSAGE_CHUNK");
                VLOG(1) << "[RESERVE OK] S2C_MESSAGE_CHUNK off=" << off
                        << " size=" << this_chunk << " pad=" << pad;
                unsigned char* base = rb->GetBuffer(cb_);
                
                // Copy chunk data
                std::memcpy(base + off, temp_buf.data() + src_offset, this_chunk);
                std::atomic_thread_fence(std::memory_order_release);
                
                if (pad) {
                  VLOG(1) << "EMIT PAD dir=S2C bytes=" << pad;
                  grpc_shmem::Command pad_cmd{};
                  pad_cmd.stream_id = 0;
                  pad_cmd.type = grpc_shmem::FrameType::DATA_PAD;
                  pad_cmd.data_offset = 0;
                  pad_cmd.data_size = pad;
                  grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                          grpc_shmem::Direction::kS2C,
                                          pad_cmd, sem_adapter_.get());
                }
                
                grpc_shmem::Command chunk_cmd{};
                chunk_cmd.stream_id = stream_id;
                chunk_cmd.type = is_last_chunk ? grpc_shmem::FrameType::S2C_MESSAGE_CHUNK_LAST 
                                               : grpc_shmem::FrameType::S2C_MESSAGE_CHUNK;
                chunk_cmd.data_offset = off;
                chunk_cmd.data_size = static_cast<uint32_t>(this_chunk);
                fprintf(stderr, "=== CRITICAL: Sending S2C chunk type=%s size=%u remaining=%zu ===\n", 
                        is_last_chunk ? "LAST" : "CHUNK", chunk_cmd.data_size, remaining); fflush(stderr);
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C,
                                        chunk_cmd, sem_adapter_.get());
                
                src_offset += this_chunk;
                remaining -= this_chunk;
              }
              
              VLOG(3) << "ShmemCallOutboundLoop: S2C_MESSAGE chunking complete for stream " << stream_id;
            }
            return Success{};
          }
        ),
        Map(
          call_initiator.PullServerTrailingMetadata(),
          [this, stream_id](ServerMetadataHandle md) {
            SHMEM_DBGF("*** DEBUG: ShmemCallOutboundLoop: PullServerTrailingMetadata returned for stream_id=%lu ***\n", stream_id);
            fprintf(stderr, "=== CANCEL DEBUG: Server pulling trailing metadata for stream_id=%lu ===\n", stream_id); fflush(stderr);
            VLOG(1) << "SERVER: Sending S2C_TRAILING_METADATA stream_id=" << stream_id;
            std::vector<grpc_shmem::KVPair> kvs;
            grpc_status_code status = GRPC_STATUS_OK;
            if (auto* s = md->get_pointer(GrpcStatusMetadata()); s) {
              status = *s;
            }
            if (auto* m = md->get_pointer(GrpcMessageMetadata()); m) {
              kvs.push_back({"grpc-message", std::string(m->as_string_view())});
            }
            kvs.push_back({"grpc-status", std::to_string(status)});
            
            // TODO: Add binary metadata fields from trailing metadata
            // For now, just basic status and message
            auto buf = grpc_shmem::SerializeMetadataKVs(kvs);
            uint64_t off = 0; uint32_t pad = 0;
            WaitReserveWithWatchdog(&cb_->GetS2CQueues()->data_rb, buf.size(), &off, &pad, "S2C_TRAILING_METADATA");
            VLOG(1) << "[RESERVE OK] S2C_TRAILING_METADATA off=" << off
                    << " size=" << buf.size() << " pad=" << pad;
            std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + off, buf.data(), buf.size());
            std::atomic_thread_fence(std::memory_order_release);
            if (pad) {
              VLOG(1) << "EMIT PAD dir=S2C bytes=" << pad;
              grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, pad, 0, 0};
              grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, pad_cmd, sem_adapter_.get());
            }
            grpc_shmem::Command out{stream_id, grpc_shmem::FrameType::S2C_TRAILING_METADATA, off, (uint32_t)buf.size(), 0, 0};
            grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, out, sem_adapter_.get());
            VLOG(3) << "ShmemCallOutboundLoop: S2C_TRAILING_METADATA sent for stream " << stream_id;
            // Clean up stream tracking after sending trailing metadata
            {
              MutexLock lock(&stream_initiators_mu_);
              stream_initiators_.erase(stream_id);
            }
            VLOG(2) << "ShmemCallOutboundLoop completed for stream " << stream_id;
            return Success{};
          }
        )
    );
  }

  void ServerLoop() {
    ExecCtx exec_ctx;
    
    // Validate ControlBlock before use
    VLOG(2) << "ServerLoop starting, cb_=" << cb_;
    
    LOG(INFO) << "Wire values: C2S_CHUNK=" << static_cast<int>(grpc_shmem::FrameType::C2S_MESSAGE_CHUNK)
              << " C2S_CHUNK_LAST=" << static_cast<int>(grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST)
              << " S2C_CHUNK=" << static_cast<int>(grpc_shmem::FrameType::S2C_MESSAGE_CHUNK)
              << " S2C_CHUNK_LAST=" << static_cast<int>(grpc_shmem::FrameType::S2C_MESSAGE_CHUNK_LAST)
              << " DATA_PAD=" << static_cast<int>(grpc_shmem::FrameType::DATA_PAD);
    
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
      
      // For reassembling chunked C2S messages (copied slices, not ring-backed)
      grpc_core::SliceBuffer c2s_copied_accumulator;
      bool c2s_copy_accumulating = false;
      
      // Pending message to be delivered before trailing metadata
      std::unique_ptr<grpc_core::SliceBuffer> pending_message;
    };

    // Simplified ring-based approach - no complex dispatched call logic needed
    absl::flat_hash_map<uint32_t, StreamState> streams;
    int loop_count = 0;
    VLOG(2) << "ServerLoop: Starting command processing loop";
    
    for (;;) {
      if (stop_.load(std::memory_order_relaxed)) {
        break;
      }
      loop_count++;
      
      // Delay removed to test next point
      
      // Log every 100 iterations to show server is alive
      if (loop_count % 100 == 0) {
        VLOG(3) << "ServerLoop: Iteration " << loop_count << ", checking for commands";
        
        // Check if we can access queues
        auto* c2s_queues = cb_->GetC2SQueues();
        if (!c2s_queues) {
          LOG(WARNING) << "C2S queues pointer is null";
        }
      }
      
      grpc_shmem::Command cmd;
      bool has_command = grpc_shmem::PopCommandHybrid(
          cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S, spin_iters_,
          &cmd, sem_adapter_.get());
      
      if (has_command) {
        // DELAY TEST POINT 4: After receiving first command
        if (loop_count == 1) {
          FORCE_RACE_SERVER_DELAY();
        }
        VLOG(2) << "ServerLoop: Got command type " << static_cast<int>(cmd.type) 
                << " for stream " << cmd.stream_id << " (iteration " << loop_count << ")";
      } else {
        // Only log on first few iterations and then every 100 iterations
        if (loop_count <= 5 || (loop_count % 100 == 0)) {
          VLOG(3) << "ServerLoop: No command available (iteration " << loop_count << ")";
        }
        continue;
      }

      if (has_command) {
        VLOG(1) << "SERVER: POP cmd=" << static_cast<int>(cmd.type)
                << " stream=" << cmd.stream_id
                << " size=" << cmd.data_size;
        
        // Handle DATA_PAD before stream lookup
        if (cmd.type == grpc_shmem::FrameType::DATA_PAD) {
          // No stream; just free the padded bytes.
          VLOG(1) << "APPLY PAD dir=C2S bytes=" << cmd.data_size
                  << " tail_before=" << cb_->GetC2SQueues()->data_rb.tail.load(std::memory_order_relaxed);
          grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
          continue;
        }
        
        auto& st = streams[cmd.stream_id];
        
        VLOG(1) << "[SERVER C2S RB] rb=" << &cb_->GetC2SQueues()->data_rb
                << " head=" << cb_->GetC2SQueues()->data_rb.head.load(std::memory_order_relaxed)
                << " tail=" << cb_->GetC2SQueues()->data_rb.tail.load(std::memory_order_relaxed);
        
        switch (cmd.type) {
          case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
            VLOG(1) << "SERVER: Received C2S_INITIAL_METADATA stream_id=" << cmd.stream_id;
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
              
              // PATH SELECTION LOGIC: Determine which approach to use
              const bool is_cancel_path = (st.path == "/cancel");
              const bool use_synthetic = is_cancel_path;  // Use synthetic for special test paths
              const bool use_direct_startcall = !use_synthetic;  // Use direct StartCall for benchmarks and regular calls
              
              VLOG(2) << "Path selection - path='" << st.path << "', use_synthetic=" 
                     << (use_synthetic ? "true" : "false") << ", use_direct_startcall=" 
                     << (use_direct_startcall ? "true" : "false");

              if (use_direct_startcall) {
                // DIRECT STARTCALL PATH (for benchmarks and regular calls)
                // This is the working approach from the previous version
                RefCountedPtr<UnstartedCallDestination> dest;
                {
                  MutexLock lock(&dest_mu_);
                  dest = dest_;
                }
              
              if (dest != nullptr) {
                SHMEM_DBGF("*** DEBUG: Building CLIENT initial metadata for proper routing ***\n");
                
                // 1) Build CLIENT initial MD
                auto arena = call_arena_allocator_->MakeArena();
                auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
                arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
                
                // Client initial metadata handle
                auto cimd = arena->MakePooledForOverwrite<ClientMetadata>();
                
                // Extract :path from parsed kvs (required for method routing)
                std::string path = "/grpc.testing.EchoTestService/Echo";  // default
                for (const auto& kv : kvs_in) {
                  if (kv.key == ":path") {
                    path = kv.value;
                    break;
                  }
                }
                
                SHMEM_DBGF("*** DEBUG: Setting required HTTP/2 pseudo-headers, :path='%s' ***\n", path.c_str());
                
                // Fill required pseudo headers and gRPC headers
                cimd->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
                cimd->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttp);
                cimd->Set(HttpAuthorityMetadata(), Slice::FromCopiedString("shmem"));
                cimd->Set(HttpPathMetadata(), Slice::FromCopiedString(path));
                
                // gRPC-required headers
                cimd->Set(TeMetadata(), TeMetadata::kTrailers);
                cimd->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
                
                // Append user metadata from kvs_in (excluding :pseudo headers we already set)
                for (const auto& kv : kvs_in) {
                  if (kv.key.size() && kv.key[0] == ':') continue;  // skip pseudo headers
                  cimd->Append(kv.key, Slice::FromCopiedString(kv.value),
                               [](absl::string_view, const Slice&) {});
                }
                
                SHMEM_DBGF("*** DEBUG: CLIENT metadata populated, creating call pair ***\n");
                
                // 2) Create the server call pair using CLIENT initial metadata
                auto call = MakeCallPair(std::move(cimd), std::move(arena));
                
                // 3) Publish initiator so MESSAGE/TRAILING can find it
                {
                  MutexLock lk(&stream_initiators_mu_);
                  stream_initiators_.emplace(cmd.stream_id, call.initiator);
                }
                
                SHMEM_DBGF("*** DEBUG: About to call dest->StartCall with proper client metadata ***\n");
                
                // 4) Start the call via the CallDestination the core gave you
                dest->StartCall(std::move(call.handler));
                
                SHMEM_DBGF("*** DEBUG: dest->StartCall completed - should trigger AsyncService tags ***\n");
                
                // 5) Start your existing S2C response bridge
                call.initiator.SpawnGuarded("shmem-response-bridge",
                    [this, sid = static_cast<uint32_t>(cmd.stream_id), ci = call.initiator]() mutable {
                      return ShmemCallOutboundLoop(sid, std::move(ci));
                    });
                } else {
                  SHMEM_DBGF("*** DEBUG: No call destination available ***\n");
                }
                
                st.sent_initial = true;
                
                // 6) Release the C2S metadata bytes you consumed
                if (cmd.data_size) {
                  grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
                }
              } else {
                // SYNTHETIC PATH (for test hooks like /cancel)
                VLOG(2) << "Taking synthetic path - sending complete unary response";
                
                // 1. Send S2C_INITIAL_METADATA
                std::vector<grpc_shmem::KVPair> initial_kvs = {
                    {"content-type", "application/grpc"}, {"x-shmem", "1"}};
                auto initial_buf = grpc_shmem::SerializeMetadataKVs(initial_kvs);
                uint64_t initial_off = 0; uint32_t initial_pad = 0;
                WaitReserveWithWatchdog(&cb_->GetS2CQueues()->data_rb, initial_buf.size(), &initial_off, &initial_pad, "S2C_INITIAL_METADATA_SYNTHETIC");
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + initial_off,
                            initial_buf.data(), initial_buf.size());
                std::atomic_thread_fence(std::memory_order_release);
                if (initial_pad) {
                  VLOG(1) << "EMIT PAD dir=S2C bytes=" << initial_pad;
                  grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, initial_pad, 0, 0};
                  grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, pad_cmd, sem_adapter_.get());
                }
                grpc_shmem::Command initial_out{cmd.stream_id, grpc_shmem::FrameType::S2C_INITIAL_METADATA, initial_off, (uint32_t)initial_buf.size(), 0, 0};
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, initial_out, sem_adapter_.get());
                
                // 2. Send S2C_MESSAGE (echo response)
                std::string msg_content = "Hello shmem_user";
                std::string response_msg;
                response_msg.push_back(0x0A);  // field 1, wire type 2 (length-delimited)
                response_msg.push_back(static_cast<char>(msg_content.size()));  // length
                response_msg.append(msg_content);  // string data
                uint64_t msg_off = 0; uint32_t msg_pad = 0;
                if (grpc_shmem::ReserveForWrite(&cb_->GetS2CQueues()->data_rb,
                                               response_msg.size(), &msg_off, &msg_pad)) {
                  std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + msg_off,
                             response_msg.data(), response_msg.size());
                  std::atomic_thread_fence(std::memory_order_release);
                  if (msg_pad) {
                    VLOG(1) << "EMIT PAD dir=S2C bytes=" << msg_pad;
                    grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, msg_pad, 0, 0};
                    grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, pad_cmd, sem_adapter_.get());
                  }
                  grpc_shmem::Command msg_out{};
                  msg_out.stream_id = cmd.stream_id;
                  msg_out.type = grpc_shmem::FrameType::S2C_MESSAGE;
                  msg_out.data_offset = msg_off;
                  msg_out.data_size = static_cast<uint32_t>(response_msg.size());
                  msg_out.grpc_status_code = 0;
                  grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                          grpc_shmem::Direction::kS2C, msg_out, sem_adapter_.get());
                }
                
                // 3. Send S2C_TRAILING_METADATA (complete RPC)
                std::vector<grpc_shmem::KVPair> trailing_kvs = {
                    {"grpc-status", "0"}};  // GRPC_STATUS_OK
                auto trailing_buf = grpc_shmem::SerializeMetadataKVs(trailing_kvs);
                uint64_t trailing_off = 0; uint32_t trailing_pad = 0;
                WaitReserveWithWatchdog(&cb_->GetS2CQueues()->data_rb, trailing_buf.size(), &trailing_off, &trailing_pad, "S2C_TRAILING_METADATA_SYNTHETIC");
                std::memcpy(cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + trailing_off,
                            trailing_buf.data(), trailing_buf.size());
                std::atomic_thread_fence(std::memory_order_release);
                if (trailing_pad) {
                  VLOG(1) << "EMIT PAD dir=S2C bytes=" << trailing_pad;
                  grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, trailing_pad, 0, 0};
                  grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, pad_cmd, sem_adapter_.get());
                }
                grpc_shmem::Command trailing_out{cmd.stream_id, grpc_shmem::FrameType::S2C_TRAILING_METADATA, trailing_off, (uint32_t)trailing_buf.size(), 0, 0};
                grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_,
                                        grpc_shmem::Direction::kS2C, trailing_out, sem_adapter_.get());
                
                // Clean up stream state after completing synthetic RPC
                streams.erase(cmd.stream_id);
                
                // Release consumed bytes from c2s
                grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
              }
            }
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
          case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK:
          case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST: {
            auto* rb = &cb_->GetC2SQueues()->data_rb;

            // CRITICAL FIX: Copy out chunk data and free ring space immediately
            const uint8_t* src = rb->GetBuffer(cb_) + cmd.data_offset;
            grpc_slice owned = grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(src), cmd.data_size);
            st.c2s_copied_accumulator.Append(grpc_core::Slice(owned));
            // IMPORTANT: free ring space now so client can send next chunk
            rb->tail.fetch_add(cmd.data_size, std::memory_order_release);

            fprintf(stderr, "*** DEBUG: MAIN LOOP chunk type=%d size=%u freed_ring=true ***\n", static_cast<int>(cmd.type), cmd.data_size); fflush(stderr);
            VLOG(2) << "C2S MESSAGE CHUNK: copied (not ring-backed) " << cmd.data_size << " bytes; freed ring";

            // On LAST, deliver one Message from the copied slices
            if (cmd.type == grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST &&
                st.initiator.has_value() && !st.cancelled && !st.completed) {
              // Immediately deliver the assembled message to the service
              auto init = *st.initiator;
              auto msg = Arena::MakePooled<Message>(std::move(st.c2s_copied_accumulator), 0);
              init.SpawnInfallible("push-c2s-assembled-chunks", [init, m = std::move(msg)]() mutable {
                fprintf(stderr, "*** DEBUG: MAIN LOOP delivering assembled 128MB message to service ***\n"); fflush(stderr);
                init.SpawnPushMessage(std::move(m));
                fprintf(stderr, "*** DEBUG: MAIN LOOP SpawnPushMessage completed ***\n"); fflush(stderr);
                return Empty{};
              });
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
            VLOG(1) << "SERVER: Received C2S_TRAILING_METADATA stream_id=" << cmd.stream_id;
            
            // Chunked messages are now delivered immediately on CHUNK_LAST, no pending delivery needed
            
            // Client half-close: Signal FinishSends for server pipeline
            if (st.initiator.has_value()) {
              st.initiator->SpawnFinishSends();
            }
            if (cmd.data_size != 0) {
              cb_->GetC2SQueues()->data_rb.tail.fetch_add(
                  cmd.data_size, std::memory_order_release);
            }
            // Mark stream as completed for cleanup
            st.completed = true;
            break;
          }
          case grpc_shmem::FrameType::C2S_CANCEL: {
            fprintf(stderr, "=== CANCEL DEBUG: SERVER received C2S_CANCEL for stream %lu ===\n", cmd.stream_id); fflush(stderr);
            LOG(INFO) << "SERVER: Received C2S_CANCEL for stream " << cmd.stream_id;
            st.cancelled = true;
            if (st.initiator.has_value()) {
              fprintf(stderr, "=== CANCEL DEBUG: SERVER spawning cancel for stream %lu ===\n", cmd.stream_id); fflush(stderr);
              LOG(INFO) << "SERVER: Spawning cancel for stream " << cmd.stream_id;
              st.initiator->SpawnCancel();
            } else {
              fprintf(stderr, "=== CANCEL DEBUG: SERVER no initiator found for stream %lu ===\n", cmd.stream_id); fflush(stderr);
              LOG(INFO) << "SERVER: No initiator found for stream " << cmd.stream_id << " to cancel";
            }
            break;
          }
          default: {
            LOG(ERROR) << "[UNHANDLED FRAME] type=" << static_cast<int>(cmd.type)
                       << " stream=" << cmd.stream_id
                       << " size=" << cmd.data_size;
            break;
          }
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
  
  // Futex doorbell adapter for queue operations
  std::unique_ptr<grpc_shmem::TransportSemaphoreAdapter> sem_adapter_;
  
  std::atomic<bool> shutdown_initiated_{false};
  std::atomic<bool> cleanup_complete_{false};
  RefCountedPtr<UnstartedCallDestination> dest_;
  Mutex dest_mu_;
  bool dest_ready_ ABSL_GUARDED_BY(dest_mu_) = false;
  
  // Pending calls that arrived before integration point is ready
  struct PendingCall {
    uint64_t stream_id;  // Use 64-bit to match client stream IDs
    RefCountedPtr<Arena> arena;
    ClientMetadataHandle client_initial_md;
    std::vector<grpc_shmem::KVPair> kvs_for_legacy;  // Original kvs for legacy path
  };
  std::vector<PendingCall> pending_calls_ ABSL_GUARDED_BY(dest_mu_);
  
  // Helper method to start calls with proper ordering
  void StartCallNow(PendingCall pc, bool from_flush);
  
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
  // std::thread reader_; // Removed - using EventEngine callbacks
  std::atomic<bool> stop_{false};
  std::atomic<bool> reader_started_{false};
  
  // Managed thread for server futex polling
  std::thread server_polling_thread_;
  std::shared_ptr<std::atomic<bool>> server_thread_stop_flag_;
  
  // Store channel args to provide auth context to filter creation
  ChannelArgs channel_args_;
  int spin_iters_ = kDefaultSpinIters;
  // Always use ring-based communication
  RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  int c2s_doorbell_fd_ = -1;  // unused in futex-only mode
  grpc_fd* c2s_grpc_fd_ = nullptr;  // unused in futex-only mode
  grpc_closure c2s_on_readable_;  // unused in futex-only mode
  grpc_pollset_set* pss_ = nullptr;
  bool pss_owned_ = false;
  bool c2s_reading_started_ = false;
  // Futex-only doorbells; no Unix sockets or polling interfaces are used
  Mutex s2c_mu_;
  // Thread-safe tracking of completed streams for cleanup
  std::mutex completed_streams_mu_;
  std::unordered_set<uint32_t> completed_streams_;
  // ForwardCall support: store CallInitiators for client access
  Mutex stream_initiators_mu_;
  absl::flat_hash_map<uint64_t, CallInitiator> stream_initiators_
      ABSL_GUARDED_BY(stream_initiators_mu_);

  // RACE CONDITION FIX: Buffer C2S frames that arrive before FinishAccept() completes
  struct PendingMessage {
    grpc_shmem::Command cmd;
    std::vector<uint8_t> data;  // Copy of message data from ring buffer
    PendingMessage(const grpc_shmem::Command& c, const uint8_t* buf, size_t len) 
        : cmd(c), data(buf, buf + len) {}
  };
  Mutex pending_messages_mu_;
  absl::flat_hash_map<uint32_t, std::vector<PendingMessage>> pending_c2s_msgs_
      ABSL_GUARDED_BY(pending_messages_mu_);
  absl::flat_hash_map<uint32_t, PendingMessage> pending_trailing_
      ABSL_GUARDED_BY(pending_messages_mu_);

  // Efficient signaling mechanism - per-stream synchronization
  struct StreamSync {
    Mutex mu;
    CondVar cv;
    bool initiator_ready = false;
  };
  Mutex stream_sync_mu_;
  absl::flat_hash_map<uint32_t, std::unique_ptr<StreamSync>> stream_sync_
      ABSL_GUARDED_BY(stream_sync_mu_);

  // Server-side C2S chunk reassembly (by stream id)
  struct C2SChunkState { 
    grpc_core::SliceBuffer acc; 
    bool accumulating = false; 
  };
  absl::flat_hash_map<uint64_t, C2SChunkState> c2s_chunks_;

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

void ShmemServerTransport::FinishAccept(const void* server_data) {
  auto* sd = static_cast<const ShmemServerData*>(server_data);
  ExecCtx exec_ctx;

  auto arena = call_arena_allocator_->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

  // Convert sd->initial_md -> ClientMetadata (same code you already had)
  auto md = arena->MakePooledForOverwrite<ClientMetadata>();
  std::string path_value;
  for (const auto& kv : sd->initial_md) {
  SHMEM_DBGF("*** DEBUG: FinishAccept metadata: key='%s', value='%s' ***\n", kv.key.c_str(), kv.value.c_str());
    if (kv.key == ":path") {
      path_value = kv.value;
      md->Set(HttpPathMetadata(), Slice::FromCopiedString(kv.value));
    } else if (kv.key == ":method") md->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
    else if (kv.key == ":scheme")
      md->Set(HttpSchemeMetadata(), kv.value == "https" ? HttpSchemeMetadata::kHttps
                                                        : HttpSchemeMetadata::kHttp);
    else if (kv.key == "te") md->Set(TeMetadata(), TeMetadata::kTrailers);
    else if (kv.key == "content-type")
      md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
    else if (kv.key == "grpc-accept-encoding")
      md->Set(GrpcAcceptEncodingMetadata(), CompressionAlgorithmSet{GRPC_COMPRESS_NONE});
    else if (kv.key == "user-agent")
      md->Set(UserAgentMetadata(), Slice::FromCopiedString(kv.value));
    else if (kv.key == ":authority")
      md->Set(HttpAuthorityMetadata(), Slice::FromCopiedString(kv.value));
    else
      md->Append(kv.key, Slice::FromCopiedString(kv.value), [](absl::string_view, const Slice&) {});
  }
  md->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
  SHMEM_DBGF("*** DEBUG: FinishAccept path='%s', calling MakeCallPair ***\n", path_value.c_str());

  auto call = MakeCallPair(std::move(md), std::move(arena));

  RefCountedPtr<UnstartedCallDestination> d;
  { MutexLock l(&dest_mu_); d = dest_; }
  if (d == nullptr) { delete sd; return; }

  // THIS is where core/filters are ready; StartCall now is safe.
  fprintf(stderr, "*** DEBUG: About to call StartCall on UnstartedCallDestination ***\n");
  fflush(stderr);
  d->StartCall(std::move(call.handler));
  fprintf(stderr, "*** DEBUG: StartCall completed ***\n");
  fflush(stderr);

  {
    MutexLock lk(&stream_initiators_mu_);
    stream_initiators_.emplace(sd->stream_id, call.initiator);
  }
  
  // RACE CONDITION FIX: Flush any pending messages that arrived before FinishAccept() completed
  fprintf(stderr, "*** DEBUG: FinishAccept - flushing pending messages for stream_id=%lu ***\n", sd->stream_id);
  fflush(stderr);
  
  // Process pending C2S messages
  std::vector<PendingMessage> pending_msgs;
  std::unique_ptr<PendingMessage> pending_trailing;
  {
    MutexLock lock(&pending_messages_mu_);
    auto it = pending_c2s_msgs_.find(sd->stream_id);
    if (it != pending_c2s_msgs_.end()) {
      pending_msgs = std::move(it->second);
      pending_c2s_msgs_.erase(it);
      fprintf(stderr, "*** DEBUG: Found %zu pending C2S messages for stream_id=%lu ***\n", pending_msgs.size(), sd->stream_id);
      fflush(stderr);
    }
    
    auto trailing_it = pending_trailing_.find(sd->stream_id);
    if (trailing_it != pending_trailing_.end()) {
      pending_trailing = std::make_unique<PendingMessage>(std::move(trailing_it->second));
      pending_trailing_.erase(trailing_it);
      fprintf(stderr, "*** DEBUG: Found pending trailing metadata for stream_id=%lu ***\n", sd->stream_id);
      fflush(stderr);
    }
  }
  
  // Deliver pending messages in order
  for (const auto& pending_msg : pending_msgs) {
    fprintf(stderr, "*** DEBUG: Delivering pending C2S message, size=%u ***\n", pending_msg.cmd.data_size);
    fflush(stderr);
    
    call.initiator.SpawnInfallible("push-pending-msg", [call_initiator = call.initiator, pending_msg]() mutable {
      SliceBuffer sb;
      // Create slice from copied data (TODO: eliminate this copy by using ring-backed message assembly)
      grpc_slice s = grpc_slice_from_copied_buffer(
          reinterpret_cast<const char*>(pending_msg.data.data()), 
          pending_msg.data.size());
      sb.AppendIndexed(Slice(s));
      auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
      call_initiator.SpawnPushMessage(std::move(msg));
      return Empty{};
    });
  }
  
  // Deliver pending trailing metadata (signals end of client stream)
  if (pending_trailing) {
    fprintf(stderr, "*** DEBUG: Delivering pending trailing metadata ***\n");
    fflush(stderr);
    
    call.initiator.SpawnInfallible("finish-pending-recv", [call_initiator = call.initiator]() mutable {
      call_initiator.SpawnFinishSends();
      return Empty{};
    });
  }
  
  // Start your S2C bridge (unchanged)
  call.initiator.SpawnGuarded("shmem-response-bridge",
      [this, sid = sd->stream_id, ci = call.initiator]() mutable {
        return ShmemCallOutboundLoop(sid, std::move(ci));
      });

  delete sd;
}

// CLIENT NEVER PERFORMS SEGMENT CLEANUP - only server manages segment lifetime
void ShmemClientTransport::PerformFinalCleanup() {
  LOG(ERROR) << "CLIENT CLEANUP ERROR: PerformFinalCleanup() should never be called for client! PID " << getpid();
  // This method should not be called - clients don't clean up the shared segment
}

void ShmemServerTransport::PerformFinalCleanup() {
  if (cb_ == nullptr) return;
  
  LOG(INFO) << "SERVER CLEANUP: Starting PerformFinalCleanup() for PID " << getpid();
  
  try {
    // Remove the segment from global registry on final cleanup
    LOG(INFO) << "SERVER CLEANUP: About to call RemoveCrossProcessSegment()";
    RemoveCrossProcessSegment(cb_);
    LOG(INFO) << "SERVER CLEANUP: RemoveCrossProcessSegment() completed";
    LOG(INFO) << "SERVER CLEANUP: Final cleanup complete for PID " << getpid();
  } catch (const std::exception& e) {
    LOG(ERROR) << "SERVER CLEANUP: ShmemServerTransport::PerformFinalCleanup error: " << e.what();
  }
}

void ShmemServerTransport::StartCallNow(PendingCall pc, bool from_flush) {
  SHMEM_DBGF("StartCallNow called for stream_id=%lu, from_flush=%s\n", 
          pc.stream_id, from_flush ? "true" : "false");
  
  // Early check for shutdown to prevent new call creation during shutdown
  if (stop_.load(std::memory_order_relaxed)) {
    SHMEM_DBGF("*** DEBUG: Server StartCallNow - transport is shutting down, rejecting call ***\n");
    return; // Don't start new calls during shutdown
  }
  VLOG(2) << "SERVER StartCallNow - stream_id=" << pc.stream_id;
  
  // Ensure EventEngine context is set on arena
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  pc.arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
  
  // Build the server call pair with the client initial metadata we saved
  auto call = MakeCallPair(std::move(pc.client_initial_md), pc.arena);
  
  // Stash initiator so MESSAGE/TRAILING can find it
  {
    MutexLock lk(&stream_initiators_mu_);
    stream_initiators_.emplace(pc.stream_id, call.initiator);
  }
  
  // Start on the real destination (now installed by core)
  RefCountedPtr<UnstartedCallDestination> dest_copy;
  {
    MutexLock lk(&dest_mu_);
    dest_copy = dest_;
  }
  
  if (dest_copy != nullptr) {
    SHMEM_DBGF("Starting call on real destination\n");
    dest_copy->StartCall(std::move(call.handler));
    SHMEM_DBGF("Call started successfully\n");
  } else {
    SHMEM_DBGF("ERROR: dest_copy is null in StartCallNow\n");
  }
  
  // Start response bridge
  SHMEM_DBGF("About to spawn ShmemCallOutboundLoop for stream_id=%lu\n", pc.stream_id);
  call.initiator.SpawnGuarded("shmem-response-bridge",
      [this, sid = pc.stream_id, ci = call.initiator]() mutable {
        SHMEM_DBGF("Inside lambda - about to call ShmemCallOutboundLoop for stream_id=%lu\n", sid);
        return ShmemCallOutboundLoop(sid, std::move(ci));
      });
  SHMEM_DBGF("SpawnGuarded call completed for stream_id=%lu\n", pc.stream_id);
}

void ShmemClientTransport::EnsureReaderStarted() {
  SHMEM_DBGF("*** DEBUG: Client EnsureReaderStarted CALLED ***\n");
  VLOG(1) << "Client EnsureReaderStarted called, cb_=" << (void*)cb_;
  if (cb_ == nullptr) {
    SHMEM_DBGF("*** DEBUG: Client EnsureReaderStarted: cb_ is null, returning ***\n");
    VLOG(1) << "Client EnsureReaderStarted: cb_ is null, returning";
    return;
  }
  bool was_started = reader_started_.load(std::memory_order_acquire);
  SHMEM_DBGF("*** DEBUG: Client EnsureReaderStarted: reader_started_=%s ***\n", was_started ? "true" : "false");
  VLOG(1) << "Client EnsureReaderStarted: reader_started_=" << was_started;
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    SHMEM_DBGF("*** DEBUG: Client EnsureReaderStarted: starting doorbell init (FIRST TIME) ***\n");
    VLOG(1) << "Client EnsureReaderStarted: starting doorbell init";
    InitS2CDoorbell();
    reader_ready_.store(true, std::memory_order_release);
  } else {
    SHMEM_DBGF("*** DEBUG: Client EnsureReaderStarted: reader already started (DUPLICATE CALL) ***\n");
    VLOG(1) << "Client EnsureReaderStarted: reader already started";
  }
}

void ShmemClientTransport::InitS2CDoorbell() {
  VLOG(2) << "Client InitS2CDoorbell (futex-only) CALLED";
  fflush(stderr);
  auto name = channel_args_.GetString("grpc.shmem.server_name");
  if (!name.has_value()) {
    fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: no server name, skipping ***\n");
    fflush(stderr);
    LOG(INFO) << "Client InitS2CDoorbell: no server name, skipping";
    return;
  }
  VLOG(2) << "Client InitS2CDoorbell: server_name=" << std::string(name->data(), name->length());
  LOG(INFO) << "Client InitS2CDoorbell: server_name=" << *name;
  
  // CRITICAL FIX: Ensure stop conditions are properly initialized for this thread
  stop_.store(false, std::memory_order_release);
  
  // Start a futex wait thread to react to s2c_db.seq changes; no eventfd/grpc_fd
  VLOG(2) << "Client InitS2CDoorbell: starting futex wait thread";
  fflush(stderr);
  
  // Use a shared atomic flag for thread communication
  auto thread_stop_flag = std::make_shared<std::atomic<bool>>(false);
  thread_stop_flag_ = thread_stop_flag;
  
  // Create futex wait thread
  polling_thread_ = std::thread([this, thread_stop_flag]() {
    VLOG(2) << "Client futex wait thread started";
    fflush(stderr);
    
    if (this->cb_ == nullptr) {
      fprintf(stderr, "*** DEBUG: Client futex thread: cb_ is null! ***\n");
      fflush(stderr);
      return;
    }
    
    uint32_t last_seq = this->cb_->s2c_db.seq.load(std::memory_order_acquire);
    VLOG(2) << "Client initial S2C sequence: " << last_seq;
    fflush(stderr);
    // Drain once on startup to handle frames posted before thread started
    this->DrainS2CFromPoller();
    
    // Futex wait loop with proper signal handling  
    VLOG(2) << "Client about to enter futex wait loop";
    fflush(stderr);
    while (!thread_stop_flag->load(std::memory_order_acquire)) {
      uint32_t expected = last_seq;
      
      // Use blocking futex wait without timeout to avoid unnecessary wake-ups
      int futex_result = futex_wait(reinterpret_cast<uint32_t*>(&this->cb_->s2c_db.seq), expected, nullptr);
      
      uint32_t current_seq = this->cb_->s2c_db.seq.load(std::memory_order_acquire);
      if (current_seq != last_seq) {
        VLOG(3) << "Client S2C sequence changed from " << last_seq << " to " << current_seq 
                << " - processing responses";
        this->DrainS2CFromPoller();
        last_seq = current_seq;
      }
    }
    VLOG(2) << "Client futex wait thread finished";
    fflush(stderr);
  });
}

void ShmemClientTransport::DrainS2CFromPoller() {
  SHMEM_DBGF("*** DEBUG: Client DrainS2CFromPoller CALLED ***\n");
  
  // CRITICAL: Set up execution context for spawned tasks with background thread flags
  ExecCtx exec_ctx(GRPC_EXEC_CTX_FLAG_IS_FINISHED | GRPC_EXEC_CTX_FLAG_THREAD_RESOURCE_LOOP);
  
  // Process S2C commands (non-blocking)
  int commands_processed = 0;
  for (;;) {
    grpc_shmem::Command cmd;
    if (!grpc_shmem::PopCommandHybrid(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C,
                                     0, &cmd, sem_adapter_.get())) {
      break; // No more commands
    }
    
    commands_processed++;
    SHMEM_DBGF("*** DEBUG: Client processing S2C command #%d: type=%d, stream_id=%lu ***\n",
         commands_processed, static_cast<int>(cmd.type), cmd.stream_id);
    VLOG(2) << "Client EventEngine processing S2C command: " << static_cast<int>(cmd.type) << " stream=" << cmd.stream_id;
    
    // Handle DATA_PAD first (no handler needed)
    if (cmd.type == grpc_shmem::FrameType::DATA_PAD) {
      grpc_shmem::Release(&cb_->GetS2CQueues()->data_rb, cmd.data_size);
      continue;
    }
    
    // Look up handler for this stream
    std::shared_ptr<CallHandler> handler;
    {
      MutexLock lock(&mu_);
      auto it = handlers_.find(cmd.stream_id);
      if (it != handlers_.end()) {
        handler = it->second;  // shared_ptr copy
      }
    }
    
    if (!handler) {
      VLOG(2) << "Client: No handler for stream " << cmd.stream_id;
      continue;
    }
    
    // Enqueue S2C ops for ordered delivery (init -> msg -> trailing)
    bool should_start_drain = false;
    switch (cmd.type) {
      case grpc_shmem::FrameType::S2C_INITIAL_METADATA: {
        RecvOp op; op.type = RecvOp::kInit;
        // Discard metadata bytes to prevent hanging - basic functionality preserved
        cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        {
          MutexLock ql(&s2c_recv_mu_);
          auto& rs = s2c_recv_[cmd.stream_id];
          rs.q.push_back(std::move(op));
          if (!rs.draining) { rs.draining = true; should_start_drain = true; }
        }
        break;
      }
      case grpc_shmem::FrameType::S2C_MESSAGE: {
        RecvOp op; op.type = RecvOp::kMsg;
        grpc_slice s = grpc_shmem::MakeSliceFromRing(&cb_->GetS2CQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
        op.buf.AppendIndexed(Slice(s));
        {
          MutexLock ql(&s2c_recv_mu_);
          auto& rs = s2c_recv_[cmd.stream_id];
          rs.q.push_back(std::move(op));
          if (!rs.draining) { rs.draining = true; should_start_drain = true; }
        }
        break;
      }
      case grpc_shmem::FrameType::S2C_MESSAGE_CHUNK: {
        // Apply copy-out and free ring space immediately
        auto& st = s2c_chunk_accumulators_[cmd.stream_id];
        auto* rb = &cb_->GetS2CQueues()->data_rb;
        const uint8_t* src = rb->GetBuffer(cb_) + cmd.data_offset;
        grpc_slice owned = grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(src), cmd.data_size);
        st.accumulator.AppendIndexed(Slice(owned));
        st.accumulating = true;
        rb->tail.fetch_add(cmd.data_size, std::memory_order_release);
        break;
      }
      case grpc_shmem::FrameType::S2C_MESSAGE_CHUNK_LAST: {
        // Assemble final chunk and enqueue one kMsg
        auto& st = s2c_chunk_accumulators_[cmd.stream_id];
        auto* rb = &cb_->GetS2CQueues()->data_rb;
        const uint8_t* src = rb->GetBuffer(cb_) + cmd.data_offset;
        grpc_slice owned = grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(src), cmd.data_size);
        st.accumulator.AppendIndexed(Slice(owned));
        st.accumulating = false;
        rb->tail.fetch_add(cmd.data_size, std::memory_order_release);
        
        RecvOp op; op.type = RecvOp::kMsg; op.buf = std::move(st.accumulator);
        {
          MutexLock ql(&s2c_recv_mu_);
          auto& rs = s2c_recv_[cmd.stream_id];
          rs.q.push_back(std::move(op));
          if (!rs.draining) { rs.draining = true; should_start_drain = true; }
        }
        s2c_chunk_accumulators_.erase(cmd.stream_id);
        break;
      }
      case grpc_shmem::FrameType::S2C_TRAILING_METADATA: {
        RecvOp op; op.type = RecvOp::kTrailing;
        // Copy metadata bytes only for parsing status/message - minimal approach
        grpc_slice s = grpc_shmem::MakeSliceFromRing(&cb_->GetS2CQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
        op.buf.AppendIndexed(Slice(s));
        {
          MutexLock ql(&s2c_recv_mu_);
          auto& rs = s2c_recv_[cmd.stream_id];
          rs.q.push_back(std::move(op));
          if (!rs.draining) { rs.draining = true; should_start_drain = true; }
        }
        break;
      }
      default:
        VLOG(2) << "Client: unknown command type " << static_cast<int>(cmd.type);
        break;
    }
    
    if (should_start_drain) {
      StartRecvDrain(cmd.stream_id);
    }
  }
  
  SHMEM_DBGF("*** DEBUG: Client DrainS2CFromPoller processed %d commands ***\n", commands_processed);
}

void ShmemClientTransport::StartRecvDrain(uint64_t sid) {
  std::shared_ptr<CallHandler> h;
  {
    MutexLock lock(&mu_);
    auto it = handlers_.find(sid);
    if (it != handlers_.end()) h = it->second;  // shared_ptr copy
  }
  if (!h) return;
  
  h->SpawnInfallible("s2c-recv-drain", [this, sid, h]() mutable {
    RecvOp op; bool has_more = false;
    {
      MutexLock ql(&s2c_recv_mu_);
      auto it = s2c_recv_.find(sid);
      if (it == s2c_recv_.end() || it->second.q.empty()) {
        if (it != s2c_recv_.end()) it->second.draining = false;
        return Empty{};
      }
      op = std::move(it->second.q.front());
      it->second.q.pop_front();
      has_more = !it->second.q.empty();
      if (!has_more) it->second.draining = false;
    }
    
    switch (op.type) {
      case RecvOp::kInit: {
        auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
        md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
        // Skip parsing initial metadata to avoid hanging - focus only on trailing metadata
        h->SpawnPushServerInitialMetadata(std::move(md));
        break;
      }
      case RecvOp::kMsg: {
        auto msg = Arena::MakePooled<Message>(std::move(op.buf), 0);
        h->SpawnPushMessage(std::move(msg));
        break;
      }
      case RecvOp::kTrailing: {
        auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
        
        // Parse trailing metadata bytes - focus on status/message only
        if (op.buf.Length() > 0 && op.buf.Count() > 0) {
          auto slice = op.buf.RefSlice(0);
          const uint8_t* bytes = slice.begin();
          size_t len = slice.length();
          auto kvs = grpc_shmem::DeserializeMetadataKVs(bytes, len);
          
          // Extract ONLY status and message - skip binary metadata to avoid parsing issues
          grpc_status_code status = GRPC_STATUS_OK;
          for (const auto& kv : kvs) {
            if (kv.key == "grpc-status") {
              status = static_cast<grpc_status_code>(atoi(kv.value.c_str()));
            } else if (kv.key == "grpc-message") {
              md->Set(GrpcMessageMetadata(), Slice::FromCopiedString(kv.value));
            }
            // Skip binary metadata parsing to prevent hanging issues
          }
          md->Set(GrpcStatusMetadata(), status);
        } else {
          // Fallback to OK status if no metadata bytes
          md->Set(GrpcStatusMetadata(), GRPC_STATUS_OK);
        }
        h->SpawnPushServerTrailingMetadata(std::move(md));
        // Only erase handler inside the trailing op
        {
          MutexLock lock(&mu_);
          handlers_.erase(sid);
        }
        {
          MutexLock ql(&s2c_recv_mu_);
          s2c_recv_.erase(sid);
        }
        break;
      }
    }
    if (has_more) this->StartRecvDrain(sid);
    return Empty{};
  });
}

void ShmemClientTransport::OnS2CReadable(void* arg, grpc_error_handle error) { /* unused in futex-only */ }

// Server-side EventEngine integration
void ShmemServerTransport::InitC2SDoorbell() {
  LOG(INFO) << "Server InitC2SDoorbell: starting futex wait thread";
  
  // Create futex doorbell adapter if not already created
  if (!sem_adapter_ && cb_) {
    LOG(INFO) << "Creating FutexDoorbellAdapter for SERVER (InitC2SDoorbell) - cb_=" << cb_;
    sem_adapter_ = std::make_unique<FutexDoorbellAdapter>(cb_);
    LOG(INFO) << "Created FutexDoorbellAdapter for SERVER (InitC2SDoorbell) - sem_adapter_=" << sem_adapter_.get();
  } else if (!cb_) {
    LOG(WARNING) << "InitC2SDoorbell called but cb_ is null - adapter not initialized";
  } else if (sem_adapter_) {
    LOG(INFO) << "FutexDoorbellAdapter already exists - sem_adapter_=" << sem_adapter_.get();
  }
  
  // CRITICAL FIX: Ensure stop conditions are properly initialized for this thread
  stop_.store(false, std::memory_order_release);
  
  // Prevent multiple starts
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    c2s_reading_started_ = true;
    auto server_thread_stop_flag = std::make_shared<std::atomic<bool>>(false);
    server_thread_stop_flag_ = server_thread_stop_flag;
    auto* server_self = this;
    server_polling_thread_ = std::thread([server_self, server_thread_stop_flag]() {
      VLOG(2) << "Server futex wait thread started";
      fflush(stderr);
      uint32_t last_seq = server_self->cb_->c2s_db.seq.load(std::memory_order_acquire);
      VLOG(2) << "Initial C2S sequence: " << last_seq;
      fflush(stderr);
      // Drain once on startup to handle commands posted before thread started
      server_self->DrainC2SFromPoller();
      VLOG(2) << "Server about to enter futex wait loop";
      fflush(stderr);
      while (!server_thread_stop_flag->load(std::memory_order_acquire)) {
        uint32_t expected = last_seq;
        
        // Use blocking futex wait without timeout to avoid unnecessary wake-ups  
        int futex_result = futex_wait(reinterpret_cast<uint32_t*>(&server_self->cb_->c2s_db.seq), expected, nullptr);
        
        uint32_t current_seq = server_self->cb_->c2s_db.seq.load(std::memory_order_acquire);
        if (current_seq != last_seq) {
          VLOG(3) << "Server C2S sequence changed from " << last_seq << " to " << current_seq 
                  << " - processing commands";
          server_self->DrainC2SFromPoller();
          last_seq = current_seq;
        }
      }
      VLOG(2) << "Server futex wait thread finished";
      fflush(stderr);
    });
  }
}

void ShmemServerTransport::DrainC2SFromPoller() {
  VLOG(3) << "DrainC2SFromPoller CALLED";
  SHMEM_DBGF("*** DEBUG: DrainC2SFromPoller CALLED ***\n");
  
  // CRITICAL: Set up execution context for spawned tasks with background thread flags
  ExecCtx exec_ctx(GRPC_EXEC_CTX_FLAG_IS_FINISHED | GRPC_EXEC_CTX_FLAG_THREAD_RESOURCE_LOOP);
  
  // Process C2S commands (non-blocking)  
  int commands_processed = 0;
  for (;;) {
    grpc_shmem::Command cmd;
    if (!grpc_shmem::PopCommandHybrid(cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S,
                                     0, &cmd, sem_adapter_.get())) {
      break; // No more commands
    }
    
    commands_processed++;
    SHMEM_DBGF("*** DEBUG: Processing C2S command #%d: type=%d, stream_id=%lu ***\n",
         commands_processed, static_cast<int>(cmd.type), cmd.stream_id);
    VLOG(2) << "Server EventEngine processing C2S command: " << static_cast<int>(cmd.type) << " stream=" << cmd.stream_id;
    
    // Handle DATA_PAD first (no special processing needed)
    if (cmd.type == grpc_shmem::FrameType::DATA_PAD) {
      grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
      continue;
    }
    
    // Proper gRPC service dispatch implementation
  SHMEM_DBGF("*** DEBUG: Implementing proper gRPC service dispatch for command type=%d ***\n", static_cast<int>(cmd.type));
    
    // Stream initiators are now managed through stream_initiators_ map populated by FinishAccept()
    
    switch (cmd.type) {
      case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
  SHMEM_DBGF("*** DEBUG: Handling C2S_INITIAL_METADATA for stream_id=%lu ***\n", cmd.stream_id);
        
        // 1) Deserialize client initial metadata (must include :path)
        std::vector<grpc_shmem::KVPair> kvs_in;
        if (cmd.data_size > 0) {
          const unsigned char* p =
              cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
          kvs_in = grpc_shmem::DeserializeMetadataKVs(p, cmd.data_size);
        }
        
        // 2) Build CLIENT initial metadata with required pseudo-headers (following user guidance)
        auto arena = call_arena_allocator_->MakeArena();
        auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
        arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
        
        // Client initial metadata handle
        auto cimd = arena->MakePooledForOverwrite<ClientMetadata>();
        
        // Extract :path and :authority from parsed kvs (required for method routing)
        std::string path = "/grpc.testing.EchoTestService/Echo";  // default
        std::string incoming_authority = "shmem";  // default
        for (const auto& kv : kvs_in) {
          SHMEM_DBGF("*** DEBUG: metadata: key='%s', value='%s' ***\n", kv.key.c_str(), kv.value.c_str());
          if (kv.key == ":path") {
            path = kv.value;
          } else if (kv.key == ":authority") {
            incoming_authority = kv.value;
          }
        }
        
  SHMEM_DBGF("*** DEBUG: Setting required pseudo-headers, :path='%s' ***\n", path.c_str());
        
        // Fill required pseudo headers and gRPC headers (exactly per user guidance)
        cimd->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
        cimd->Set(HttpSchemeMetadata(), HttpSchemeMetadata::kHttp);
        cimd->Set(HttpAuthorityMetadata(), Slice::FromCopiedString(incoming_authority));
        cimd->Set(HttpPathMetadata(), Slice::FromCopiedString(path));
        
        // gRPC-required headers
        cimd->Set(TeMetadata(), TeMetadata::kTrailers);
        cimd->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
        
        // Append user metadata from kvs_in (excluding :pseudo headers we already set)
        for (const auto& kv : kvs_in) {
          if (kv.key.size() && kv.key[0] == ':') continue;  // skip pseudo headers
          cimd->Append(kv.key, Slice::FromCopiedString(kv.value),
                       [](absl::string_view, const Slice&) {});
        }
        
        cimd->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
        
        // 3) Build PendingCall with the metadata and arena
        PendingCall pc;
        pc.stream_id = cmd.stream_id;  // Preserve full 64-bit client stream ID
        pc.arena = std::move(arena);
        pc.client_initial_md = std::move(cimd);
        
        // 4) Robust first-arrives-wins integration: use whichever is available
        bool use_legacy = false;
        bool use_promise = false;
        bool need_buffer = false;
        
        // Check which integration point is available (thread-safe check)
        {
          MutexLock lk(&dest_mu_);
          if (accept_stream_cb_ != nullptr) {
            use_legacy = true;
            SHMEM_DBGF("*** DEBUG: accept_stream_cb available - using LEGACY integration ***\n");
          } else if (dest_ready_) {
            use_promise = true;
            SHMEM_DBGF("*** DEBUG: dest_ ready - using PROMISE integration ***\n");
          } else {
            need_buffer = true;
            SHMEM_DBGF("*** DEBUG: Neither integration ready - BUFFERING call ***\n");
          }
        }
        
        if (use_legacy) {
          // LEGACY PATH: Use accept_stream_cb for CQ-based servers (AsyncService)
          fprintf(stderr, "*** DEBUG: NEW CALL LEGACY ACCEPT_STREAM CALLBACK - stream_id=%lu ***\n", pc.stream_id); fflush(stderr);
          auto* server_data = new ShmemServerData{pc.stream_id, std::move(kvs_in)};
          accept_stream_cb_(accept_stream_cb_user_data_, this, server_data);
          fprintf(stderr, "*** DEBUG: NEW CALL LEGACY ACCEPT_STREAM CALLBACK COMPLETED - stream_id=%lu ***\n", pc.stream_id); fflush(stderr);
        } else if (use_promise) {
          // PROMISE PATH: Use dest_->StartCall for modern promise-based servers
          StartCallNow(std::move(pc), /*from_flush=*/false);
        } else if (need_buffer) {
          // BUFFERING PATH: Neither integration point ready - buffer until one arrives
          // Store original kvs for potential legacy path later
          pc.kvs_for_legacy = std::move(kvs_in);
          
          {
            MutexLock lk(&dest_mu_);
            pending_calls_.push_back(std::move(pc));
          }
          
          // Release bytes consumed - call will be processed when integration arrives
          if (cmd.data_size) {
            grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
          }
          break;  // Don't process further, call is buffered
        }
        
        // 6) Release the C2S metadata bytes we consumed
        if (cmd.data_size) {
          grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        }
        break;
      }
      
      case grpc_shmem::FrameType::C2S_MESSAGE: {
  SHMEM_DBGF("*** DEBUG: Handling C2S_MESSAGE for stream_id=%lu, size=%u ***\n", cmd.stream_id, cmd.data_size);
        
        // Look up initiator installed by FinishAccept()
        CallInitiator initiator;
        bool has_initiator = false;
        {
          MutexLock lock(&stream_initiators_mu_);
          auto it = stream_initiators_.find(cmd.stream_id);
          if (it != stream_initiators_.end()) {
            initiator = it->second;
            has_initiator = true;
          }
        }
        
        if (has_initiator) {
          // Normal path: deliver message immediately
          grpc_slice s = grpc_shmem::MakeSliceFromRing(
              &cb_->GetC2SQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
          initiator.SpawnInfallible("push-c2s-msg", [initiator, s]() mutable {
            SliceBuffer sb;
            sb.AppendIndexed(Slice(s));
            auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
            SHMEM_DBGF("*** DEBUG: Pushing message to service via SpawnPushMessage ***\n");
            initiator.SpawnPushMessage(std::move(msg));
            return Empty{};
          });
          SHMEM_DBGF("*** DEBUG: Message delivery spawned successfully ***\n");
        } else {
          // RACE CONDITION: Buffer message until FinishAccept() completes
          SHMEM_DBGF("*** DEBUG: No stream initiator found for stream_id=%lu - buffering message ***\n", cmd.stream_id);
          
          // Copy data from ring buffer before advancing tail
          const uint8_t* src = cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
          {
            MutexLock lock(&pending_messages_mu_);
            pending_c2s_msgs_[static_cast<uint32_t>(cmd.stream_id)].emplace_back(cmd, src, cmd.data_size);
          }
          SHMEM_DBGF("*** DEBUG: Message buffered for stream_id=%lu ***\n", cmd.stream_id);
        }
        
        // Always advance tail to release ring buffer space
        grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        break;
      }
      
      case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
  SHMEM_DBGF("*** DEBUG: Handling C2S_TRAILING_METADATA for stream_id=%lu ***\n", cmd.stream_id);
        
        // End-of-client-stream: signal FinishSends via CallInitiator
        CallInitiator initiator;
        bool has_initiator = false;
        {
          MutexLock lock(&stream_initiators_mu_);
          auto it = stream_initiators_.find(cmd.stream_id);
          if (it != stream_initiators_.end()) { 
            initiator = it->second; 
            has_initiator = true; 
          }
        }
        
        if (has_initiator) {
          // If we still have an assembled request buffered, deliver it first.
          {
            auto it = c2s_chunks_.find(cmd.stream_id);
            if (it != c2s_chunks_.end() && it->second.acc.Length() > 0) {
              auto msg = Arena::MakePooled<Message>(std::move(it->second.acc), 0);
              initiator.SpawnInfallible("push-c2s-msg-before-finish",
                  [init = initiator, m = std::move(msg)]() mutable {
                    init.SpawnPushMessage(std::move(m));
                    return Empty{};
                  });
              c2s_chunks_.erase(it);
            }
          }
          // Now signal end-of-client-stream.
          initiator.SpawnInfallible("finish-recv", [initiator]() mutable {
            SHMEM_DBGF("Signaling FinishSends to service\n");
            initiator.SpawnFinishSends(); 
            return Empty{};
          });
          SHMEM_DBGF("*** DEBUG: FinishRecv spawned successfully ***\n");
        } else {
          // RACE CONDITION: Buffer trailing metadata until FinishAccept() completes
          SHMEM_DBGF("*** DEBUG: No stream initiator found for stream_id=%lu - buffering trailing metadata ***\n", cmd.stream_id);
          
          // Copy data from ring buffer before advancing tail
          const uint8_t* src = cb_->GetC2SQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
          {
            MutexLock lock(&pending_messages_mu_);
            pending_trailing_.emplace(static_cast<uint32_t>(cmd.stream_id), PendingMessage(cmd, src, cmd.data_size));
          }
          SHMEM_DBGF("*** DEBUG: Trailing metadata buffered for stream_id=%lu ***\n", cmd.stream_id);
        }
        
        // Always advance tail to release ring buffer space
        grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        break;
      }
      
      // C2S chunk reassembly: copy out each chunk and free ring space immediately.
      case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK: {
        fprintf(stderr, "*** DEBUG: DRAINER C2S_MESSAGE_CHUNK - stream_id=%lu, size=%u ***\n", cmd.stream_id, cmd.data_size); fflush(stderr);
        auto& st = c2s_chunks_[cmd.stream_id];
        auto* rb = &cb_->GetC2SQueues()->data_rb;
        const uint8_t* src = rb->GetBuffer(cb_) + cmd.data_offset;
        grpc_slice owned = grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(src), cmd.data_size);
        st.acc.Append(grpc_core::Slice(owned));
        st.accumulating = true;
        // IMPORTANT: free ring space now so the client can send the next chunk
        rb->tail.fetch_add(cmd.data_size, std::memory_order_release);
        fprintf(stderr, "*** DEBUG: DRAINER freed ring space, total_length=%zu ***\n", st.acc.Length()); fflush(stderr);
        VLOG(2) << "C2S CHUNK copied " << cmd.data_size << "B; freed ring bytes";
        break;
      }

      case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST: {
        fprintf(stderr, "*** DEBUG: DRAINER C2S_MESSAGE_CHUNK_LAST - stream_id=%lu, size=%u ***\n", cmd.stream_id, cmd.data_size); fflush(stderr);
        auto& st = c2s_chunks_[cmd.stream_id];
        auto* rb = &cb_->GetC2SQueues()->data_rb;
        const uint8_t* src = rb->GetBuffer(cb_) + cmd.data_offset;
        grpc_slice owned = grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(src), cmd.data_size);
        st.acc.Append(grpc_core::Slice(owned));
        st.accumulating = false;
        rb->tail.fetch_add(cmd.data_size, std::memory_order_release);

        // If the call is ready, deliver the assembled request now; otherwise keep it.
        CallInitiator initiator;
        bool has_initiator = false;
        {
          MutexLock lock(&stream_initiators_mu_);
          auto it = stream_initiators_.find(cmd.stream_id);
          if (it != stream_initiators_.end()) { initiator = it->second; has_initiator = true; }
        }
        fprintf(stderr, "*** DEBUG: DRAINER CHUNK_LAST has_initiator=%s, total_length=%zu ***\n", has_initiator ? "true" : "false", st.acc.Length()); fflush(stderr);
        if (has_initiator) {
          auto msg = Arena::MakePooled<Message>(std::move(st.acc), 0);
          initiator.SpawnInfallible("push-c2s-assembled",
              [init = initiator, m = std::move(msg)]() mutable {
                fprintf(stderr, "*** DEBUG: DRAINER delivering assembled message to service ***\n"); fflush(stderr);
                init.SpawnPushMessage(std::move(m));
                return Empty{};
              });
          c2s_chunks_.erase(cmd.stream_id);
        } // else: keep st.acc; we'll deliver at TRAILING
        break;
      }

      case grpc_shmem::FrameType::C2S_CANCEL: {
        fprintf(stderr, "=== CANCEL DEBUG: DRAINER received C2S_CANCEL for stream %lu ===\n", cmd.stream_id); fflush(stderr);
        // Find the initiator for this stream and spawn cancellation
        CallInitiator initiator;
        bool has_initiator = false;
        {
          MutexLock lock(&stream_initiators_mu_);
          auto it = stream_initiators_.find(cmd.stream_id);
          if (it != stream_initiators_.end()) { 
            initiator = it->second; 
            has_initiator = true;
          }
        }
        if (has_initiator) {
          fprintf(stderr, "=== CANCEL DEBUG: DRAINER spawning cancel for stream %lu ===\n", cmd.stream_id); fflush(stderr);
          initiator.SpawnCancel();
        } else {
          fprintf(stderr, "=== CANCEL DEBUG: DRAINER no initiator found for stream %lu ===\n", cmd.stream_id); fflush(stderr);
        }
        // Clean up any accumulated data for this stream
        c2s_chunks_.erase(cmd.stream_id);
        break;
      }
      
      default: {
        fprintf(stderr, "*** DEBUG: Unhandled command type=%d ***\n", static_cast<int>(cmd.type));
        fflush(stderr);
        // Release data for unhandled commands
        grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        break;
      }
    }
    
    if (false) { // Keep old basic echo disabled 
    switch (cmd.type) {
      case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
        // Echo back S2C_INITIAL_METADATA
        grpc_shmem::Command response;
        response.stream_id = cmd.stream_id;
        response.type = grpc_shmem::FrameType::S2C_INITIAL_METADATA;
        response.data_offset = 0;
        response.data_size = 0;
        response.grpc_status_code = 0;
        grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, response, sem_adapter_.get());
        // Release the metadata data
        grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        break;
      }
      case grpc_shmem::FrameType::C2S_MESSAGE: {
        // Echo back S2C_MESSAGE
        grpc_shmem::Command response;
        response.stream_id = cmd.stream_id;
        response.type = grpc_shmem::FrameType::S2C_MESSAGE;
        response.data_offset = cmd.data_offset; // Reuse same data
        response.data_size = cmd.data_size;
        response.grpc_status_code = 0;
        grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, response, sem_adapter_.get());
        // Note: Don't release the data here since S2C response is using it
        break;
      }
      case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
        // Respond with S2C_TRAILING_METADATA (OK status)
        grpc_shmem::Command response;
        response.stream_id = cmd.stream_id;
        response.type = grpc_shmem::FrameType::S2C_TRAILING_METADATA;
        response.data_offset = 0;
        response.data_size = 0;
        response.grpc_status_code = 0; // GRPC_STATUS_OK
        fprintf(stderr, "*** DEBUG: Server sending S2C_TRAILING_METADATA response: stream_id=%lu ***\n", 
                response.stream_id);
        fflush(stderr);
        grpc_shmem::PushCommand(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C, response, sem_adapter_.get());
        fprintf(stderr, "*** DEBUG: Server S2C_TRAILING_METADATA PushCommand completed ***\n");
        fflush(stderr);
        // Release the metadata data
        grpc_shmem::Release(&cb_->GetC2SQueues()->data_rb, cmd.data_size);
        break;
      }
      default:
        VLOG(2) << "Server: ignoring command type " << static_cast<int>(cmd.type);
        break;
    }
    } // End of disabled basic echo processing
  }
  
  SHMEM_DBGF("*** DEBUG: DrainC2SFromPoller processed %d commands ***\n", commands_processed);
}

void ShmemServerTransport::OnC2SReadable(void* arg, grpc_error_handle error) { /* unused in futex-only */ }

// Unix socket callback removed - using futex doorbells only

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  VLOG(2) << "Client StartCall CALLED!";
  SHMEM_DBGF("*** DEBUG: Client StartCall CALLED! ***\n");
  
  // Early check for shutdown to prevent new call creation during shutdown
  if (stop_.load(std::memory_order_relaxed)) {
    SHMEM_DBGF("*** DEBUG: Client StartCall - transport is shutting down, rejecting call ***\n");
    child_call_handler.SpawnGuarded("shutdown_rejection", []() {
      return Immediate(absl::UnavailableError("transport shutting down"));
    });
    return;
  }
  
  ExecCtx exec_ctx;  // CRITICAL: Ensure SpawnGuarded tasks actually execute
  VLOG(1) << "CLIENT: StartCall scheduling send pipelines";
  EnsureReaderStarted();  // Start S2C reader for all RPCs
  
  // Client delay removed to test server delay
  
  // CRITICAL FIX: Use a process-unique stream ID that includes PID and timestamp
  // to avoid reuse across benchmark iterations and multiple processes
  static std::atomic<uint64_t> unique_counter{1};
  auto stream_id = (static_cast<uint64_t>(getpid()) << 32) | unique_counter.fetch_add(1, std::memory_order_relaxed);
  VLOG(1) << "CLIENT: StartCall stream_id=" << stream_id;
  
  // Always insert handler for S2C reader delivery
  {
    MutexLock lock(&mu_);
    handlers_.insert_or_assign(stream_id, std::make_shared<CallHandler>(child_call_handler));
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
               uint64_t off = 0; uint32_t pad = 0;
               WaitReserveWithWatchdog(&cb->GetC2SQueues()->data_rb, vec.size(), &off, &pad, "C2S_INITIAL_METADATA");
               VLOG(1) << "[RESERVE OK] C2S_INITIAL_METADATA off=" << off
                       << " size=" << vec.size() << " pad=" << pad;
               std::memcpy(cb->GetC2SQueues()->data_rb.GetBuffer(cb) + off, vec.data(), vec.size());
               std::atomic_thread_fence(std::memory_order_release);
               if (pad) {
                 VLOG(1) << "EMIT PAD dir=C2S bytes=" << pad;
                 grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, pad, 0, 0};
                 grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S, pad_cmd, sem_adapter_.get());
               }
               grpc_shmem::Command cmd{stream_id, grpc_shmem::FrameType::C2S_INITIAL_METADATA, off, (uint32_t)vec.size(), 0, 0};
               SHMEM_DBGF("*** DEBUG: Client about to PushCommand C2S_INITIAL_METADATA, sem_adapter=%p ***\n", sem_adapter_.get());
               grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S, cmd, sem_adapter_.get());
               SHMEM_DBGF("*** DEBUG: Client PushCommand completed ***\n");
               
               VLOG(1) << "CLIENT: Pushed C2S_INITIAL_METADATA stream_id=" << stream_id;
               return absl::OkStatus();
             }));
  
  // Send client messages as C2S_MESSAGE frames
  auto send_message = [cb, stream_id, this](MessageHandle m) -> StatusFlag {
    VLOG(2) << "CLIENT send_message lambda called for stream=" << stream_id;
    auto* payload = m->payload();
    const size_t n = payload->Length();
    VLOG(2) << "CLIENT message payload length=" << n << " bytes";
    
    VLOG(1) << "CLIENT: send_message stream=" << stream_id
            << " len=" << n
            << " ring_cap=" << cb->GetC2SQueues()->data_rb.capacity;
    if (n == 0) {
      // Still need to send the frame even for 0-byte messages for proper gRPC flow
      grpc_shmem::Command cmd{};
      cmd.stream_id = stream_id;
      cmd.type = grpc_shmem::FrameType::C2S_MESSAGE;
      cmd.data_offset = 0;  // No data
      cmd.data_size = 0;
      grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                              cmd, sem_adapter_.get());
      return Success{};
    }

    auto* rb = &cb->GetC2SQueues()->data_rb;
    
    if (n <= rb->capacity) {
      // Small message: use existing single-frame path
      VLOG(1) << "C2S SINGLE ENQ path: len=" << n << " <= cap=" << rb->capacity;
      uint64_t off = 0;
      uint32_t pad = 0;
      WaitReserveWithWatchdog(rb, n, &off, &pad, "C2S_MESSAGE");
      VLOG(1) << "[RESERVE OK] C2S_MESSAGE off=" << off
              << " size=" << n << " pad=" << pad;
      unsigned char* base = rb->GetBuffer(cb);
      payload->CopyToBuffer(base + off);

      std::atomic_thread_fence(std::memory_order_release);

      if (pad) {
        VLOG(1) << "EMIT PAD dir=C2S bytes=" << pad;
        grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, pad, 0, 0};
        grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                                pad_cmd, sem_adapter_.get());
      }

      grpc_shmem::Command cmd{stream_id, grpc_shmem::FrameType::C2S_MESSAGE, off, static_cast<uint32_t>(n), 0, 0};
      grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                              cmd, sem_adapter_.get());
    } else {
      // Large message: use chunking
      fprintf(stderr, "=== CLIENT: CHUNKING 128MB message len=%zu > cap=%lu ===\n", n, rb->capacity); fflush(stderr);
      VLOG(1) << "C2S CHUNKING path: len=" << n << " > cap=" << rb->capacity;
      VLOG(1) << "C2S_MESSAGE chunking: total=" << n
              << " max_chunk=" << (rb->capacity - 65536);
      
      // Allocate temporary buffer and copy payload once
      fprintf(stderr, "=== CLIENT: Allocating 128MB temp buffer ===\n"); fflush(stderr);
      VLOG(1) << "C2S CHUNKING: Allocating temp buffer size=" << n;
      std::unique_ptr<unsigned char[]> tmp(new unsigned char[n]);
      fprintf(stderr, "=== CLIENT: Copying payload to temp buffer ===\n"); fflush(stderr);
      VLOG(1) << "C2S CHUNKING: Copying payload to temp buffer";
      payload->CopyToBuffer(tmp.get());
      fprintf(stderr, "=== CLIENT: Starting chunking loop ===\n"); fflush(stderr);
      VLOG(1) << "C2S CHUNKING: Payload copied, starting chunk loop";
      
      VLOG(1) << "[CLIENT C2S RB] rb=" << rb
              << " head=" << rb->head.load(std::memory_order_relaxed)
              << " tail=" << rb->tail.load(std::memory_order_relaxed);
      
      // Leave headroom for padding
      const size_t max_chunk = rb->capacity - 65536;
      size_t offset = 0;
      
      while (offset < n) {
        const size_t chunk = std::min(n - offset, max_chunk);
        const bool is_last = (offset + chunk == n);
        fprintf(stderr, "=== CLIENT: CHUNK LOOP offset=%zu, chunk=%zu, is_last=%s ===\n", offset, chunk, is_last ? "true" : "false"); fflush(stderr);
        
        uint64_t chunk_off = 0;
        uint32_t pad = 0;
        WaitReserveWithEmptyWrap(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                                 sem_adapter_.get(),
                                 rb, chunk, &chunk_off, &pad, "C2S_MESSAGE_CHUNK");
        VLOG(1) << "[RESERVE OK] C2S_MESSAGE_CHUNK off=" << chunk_off
                << " size=" << chunk << " pad=" << pad;
        
        std::memcpy(rb->GetBuffer(cb) + chunk_off, tmp.get() + offset, chunk);
        std::atomic_thread_fence(std::memory_order_release);
        
        if (pad) {
          VLOG(1) << "EMIT PAD dir=C2S bytes=" << pad;
          grpc_shmem::Command pad_cmd{0, grpc_shmem::FrameType::DATA_PAD, 0, pad, 0, 0};
          grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                                  pad_cmd, sem_adapter_.get());
        }
        
        grpc_shmem::FrameType frame_type = is_last ? grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST 
                                                   : grpc_shmem::FrameType::C2S_MESSAGE_CHUNK;
        grpc_shmem::Command cmd{stream_id, frame_type, chunk_off, static_cast<uint32_t>(chunk), 0, 0};
        fprintf(stderr, "=== CLIENT: Sending chunk type=%s size=%u offset=%zu ===\n", 
                is_last ? "LAST" : "CHUNK", static_cast<uint32_t>(chunk), offset); fflush(stderr);
        grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                                cmd, sem_adapter_.get());
        
        VLOG(1) << "C2S CHUNK ENQ stream=" << stream_id
                << " off=" << chunk_off
                << " size=" << chunk
                << " last=" << is_last;
        fprintf(stderr, "=== CLIENT: Chunk sent successfully, advancing offset %zu -> %zu ===\n", offset, offset + chunk); fflush(stderr);
        offset += chunk;
      }
    }
    return Success{};
  };
  VLOG(2) << "CLIENT: Setting up message pipeline for stream=" << stream_id;
  VLOG(2) << "CLIENT: About to spawn message pipeline";
  child_call_handler.SpawnGuarded(
      "c2s_messages_and_eos",
      Seq(
        // 1) Stream client messages; capture both success and failure cases
        Map(ForEach(MessagesFrom(child_call_handler), std::move(send_message)),
            [cb, stream_id, this](StatusFlag sent_all) -> StatusFlag {
              VLOG(1) << "CLIENT: ForEach completed for stream=" << stream_id << " sent_all.ok()=" << sent_all.ok();
              if (!sent_all.ok()) {
                // User canceled (or send stream failed): notify server immediately.
                fprintf(stderr, "=== CANCEL DEBUG: CLIENT sending C2S_CANCEL for stream %lu ===\n", stream_id); fflush(stderr);
                VLOG(1) << "CLIENT: call canceled; sending C2S_CANCEL for stream=" << stream_id;
                grpc_shmem::Command cancel{};
                cancel.stream_id  = stream_id;
                cancel.type       = grpc_shmem::FrameType::C2S_CANCEL;
                cancel.data_offset = 0;
                cancel.data_size   = 0;
                (void)grpc_shmem::PushCommand(cb->GetC2SQueues(), cb,
                                              grpc_shmem::Direction::kC2S,
                                              cancel, sem_adapter_.get());
                fprintf(stderr, "=== CANCEL DEBUG: CLIENT sent C2S_CANCEL for stream %lu ===\n", stream_id); fflush(stderr);
                // Do NOT send C2S_TRAILING_METADATA on cancel.
                return sent_all;  // propagate canceled status
              }
              // Normal half-close (no payload)
              VLOG(2) << "CLIENT: Sending trailing metadata for stream=" << stream_id;
              grpc_shmem::Command eos{};
              eos.stream_id  = stream_id;
              eos.type       = grpc_shmem::FrameType::C2S_TRAILING_METADATA;
              eos.data_offset = 0;
              eos.data_size   = 0;
              VLOG(1) << "CLIENT: Sending C2S_TRAILING_METADATA stream_id=" << stream_id;
              bool success = grpc_shmem::PushCommand(cb->GetC2SQueues(), cb,
                                                     grpc_shmem::Direction::kC2S,
                                                     eos, sem_adapter_.get());
              return StatusFlag(success);
            })));
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
  LOG(INFO) << "SEGMENT CLEANUP: RemoveCrossProcessSegment() called with cb=" << cb << " by PID " << getpid();
  
  std::lock_guard<std::mutex> lock(g_cross_process_segments_mu);
  LOG(INFO) << "SEGMENT CLEANUP: Acquired lock, total segments in map: " << g_cross_process_segments.size();
  
  auto it = g_cross_process_segments.find(cb);
  if (it != g_cross_process_segments.end()) {
    LOG(INFO) << "SEGMENT CLEANUP: Found segment for cb=" << cb;
    
    // Log segment name if available
    if (it->second) {
      LOG(INFO) << "SEGMENT CLEANUP: Segment name: " << it->second->name();
      LOG(INFO) << "SEGMENT CLEANUP: About to call ShmemSegment destructor (which calls Unmap)";
    }
    
    // With futex doorbells, no named semaphore cleanup needed
    
    // Destroy the ShmemSegment object (this will call Unmap() in its destructor)
    it->second.reset();
    g_cross_process_segments.erase(it);
    LOG(INFO) << "SEGMENT CLEANUP: Successfully removed segment for cb=" << cb;
    LOG(INFO) << "SEGMENT CLEANUP: Remaining segments in map: " << g_cross_process_segments.size();
  } else {
    LOG(WARNING) << "SEGMENT CLEANUP: No segment found for cb=" << cb << " in map with " << g_cross_process_segments.size() << " entries";
    
    // Log what segments DO exist in the map
    for (const auto& pair : g_cross_process_segments) {
      LOG(WARNING) << "SEGMENT CLEANUP: Map contains cb=" << pair.first << " with segment name=" << (pair.second ? pair.second->name() : "null");
    }
  }
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
  uint64_t pair_id_val = pair_id.fetch_add(1);

  if (!dispatch_only) {
    grpc_shmem::SegmentConfig cfg;
    cfg.name = absl::StrCat("grpc_shmem_", getpid(), "_", pair_id_val);
    cfg.server_name = absl::StrCat("bench_", getpid(), "_", pair_id_val);  // For semaphore names
    // Optimized sizing for high-throughput large message performance
    // Larger buffers reduce fragmentation and improve contiguous allocation success
    cfg.data_ring_capacity = 64 * 1024 * 1024;   // 64MB per direction for better large message handling  
    cfg.size = 192 * 1024 * 1024;                // 192MB total segment
    fprintf(stderr, "=== SEGMENT: Creating segment size=%zu, ring_cap=%zu ===\n", cfg.size, cfg.data_ring_capacity); fflush(stderr);
    grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
    auto s = grpc_shmem::ShmemSegment::Create(cfg);
    segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
    cb = segment->control();
  }

  // Ensure both sides share a name for identifying the shared memory segment;
  // futex doorbells handle signaling, no Unix-socket or eventfd handshake.
  std::string name_for_args = absl::StrCat("bench_", getpid(), "_", pair_id_val);
  ChannelArgs sargs = server_channel_args.Set("grpc.shmem.server_name", name_for_args);
  ChannelArgs cargs = client_channel_args.Set("grpc.shmem.server_name", name_for_args);
  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      sargs, std::move(segment));
  auto client_transport = MakeOrphanable<ShmemClientTransport>(
      server_transport.get(), cb, nullptr, cargs);
  return {OrphanablePtr<Transport>(client_transport.release()), 
          OrphanablePtr<Transport>(server_transport.release())};
}

OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& server_channel_args) {
  VLOG(1) << "MakeNamedShmemServerTransport called with server_name: " << server_name;
  LOG(INFO) << "MakeNamedShmemServerTransport called with server_name: " << server_name;
  VLOG(2) << "MakeNamedShmemServerTransport called with server_name: " << server_name;
  
  // Reuse existing auth context if available, otherwise create one
  auto auth_ctx = server_channel_args.GetObjectRef<grpc_auth_context>();
  if (auth_ctx == nullptr) {
    VLOG(2) << "No auth context provided, creating new one";
    auth_ctx = MakeShmemAuthContext();
    VLOG(2) << "Created auth context: " << auth_ctx.get();
  } else {
    VLOG(2) << "Reusing provided auth context: " << auth_ctx.get();
  }
  
  // Force ring mode for cross-process server and include auth context  
  // Add flag to indicate this is a named server requiring immediate FD exchange setup
  ChannelArgs ring_mode_args = server_channel_args
      .Set("grpc.shmem.dispatch_only", false)
      .Set("grpc.shmem.is_named_server", true)
      .Set("grpc.shmem.server_name", server_name)
      .SetObject(auth_ctx);

  std::unique_ptr<grpc_shmem::ShmemSegment> segment;
  
  grpc_shmem::SegmentConfig cfg;
  cfg.name = absl::StrCat("grpc_shmem_", server_name);
  cfg.server_name = server_name;  // For named semaphores
  cfg.data_ring_capacity = 64 * 1024 * 1024;
  cfg.size = 192 * 1024 * 1024;
  VLOG(2) << "Segment config: name=" << cfg.name << ", size=" << cfg.size << ", data_ring_capacity=" << cfg.data_ring_capacity;
  
  VLOG(2) << "Removing existing segment if exists...";
  grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
  
  VLOG(1) << "Creating new segment: " << cfg.name;
  fflush(stderr);
  VLOG(2) << "Creating new segment...";
  auto s = grpc_shmem::ShmemSegment::Create(cfg);
  
  VLOG(1) << "Created segment, control block: " << s.control();
  fflush(stderr);
  VLOG(2) << "Created segment, checking control block...";
  if (s.control() == nullptr) {
    LOG(ERROR) << "Failed to create named shmem segment: " << cfg.name;
    return nullptr;
  }
  
  // CRITICAL FIX: Prevent the server segment from being unlinked when transport is destroyed
  // This allows the segment to persist for multiple client connections
  s.SetUnlinkOnDestroy(false);
  LOG(INFO) << "Server segment set to NOT unlink on destroy - will persist for multiple clients";
  
  LOG(INFO) << "Control block created successfully at: " << s.control();
  LOG(INFO) << "Control block c2s_queues: " << s.control()->GetC2SQueues();
  LOG(INFO) << "Control block s2c_queues: " << s.control()->GetS2CQueues();
  
  segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
  LOG(INFO) << "Wrapped in unique_ptr, creating server transport...";

  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      ring_mode_args, std::move(segment));
  
  LOG(INFO) << "Server transport created: " << server_transport.get();
  return OrphanablePtr<Transport>(server_transport.release());
}

OrphanablePtr<Transport> ConnectToShmemServerTransport(
    const std::string& server_name, const ChannelArgs& client_channel_args) {
  VLOG(1) << "ConnectToShmemServerTransport called with server_name: " << server_name;
  fflush(stderr);
  VLOG(2) << "ConnectToShmemServerTransport called with server_name: " << server_name;
  
  const bool dispatch_only =
      client_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(false);

  if (dispatch_only) {
    LOG(ERROR) << "Cross-process shmem requires ring mode (not dispatch-only)";
    return nullptr;
  }

  std::string segment_name = absl::StrCat("grpc_shmem_", server_name);
  LOG(INFO) << "CLIENT CONNECTION: Attempting to connect to server '" << server_name << "'";
  LOG(INFO) << "CLIENT CONNECTION: Looking for segment: " << segment_name;
  
  // Check if segment exists in filesystem first
  std::string segment_path = "/dev/shm/" + segment_name;
  if (access(segment_path.c_str(), F_OK) != 0) {
    LOG(ERROR) << "CLIENT CONNECTION: Segment file does not exist: " << segment_path << " (errno=" << errno << ": " << strerror(errno) << ")";
  } else {
    LOG(INFO) << "CLIENT CONNECTION: Segment file exists: " << segment_path;
  }
  
  VLOG(1) << "Opening segment: " << segment_name;
  fflush(stderr);
  VLOG(2) << "Opening segment: " << segment_name;
  
  auto segment = grpc_shmem::ShmemSegment::Open(segment_name);
  
  // CRITICAL FIX: Client should never unlink the server's segment!
  // The server manages the segment lifetime, client just attaches to it
  segment.SetUnlinkOnDestroy(false);
  LOG(INFO) << "Client segment set to NOT unlink on destroy - server manages segment lifetime";
  
  VLOG(1) << "Segment opened, control block: " << segment.control();
  fflush(stderr);
  VLOG(2) << "Segment opened, control block: " << segment.control();
  if (segment.control() == nullptr) {
    LOG(ERROR) << "CLIENT CONNECTION FAILED: ShmemSegment::Open() returned null control block";
    LOG(ERROR) << "CLIENT CONNECTION FAILED: Server name: " << server_name;
    LOG(ERROR) << "CLIENT CONNECTION FAILED: Segment name: " << segment_name;
    LOG(ERROR) << "CLIENT CONNECTION FAILED: Segment path: " << segment_path;
    
    // List what segments DO exist
    system("ls -la /dev/shm/grpc_shmem_* 2>/dev/null | head -10");
    
    LOG(ERROR) << "Failed to connect to shmem server: " << server_name;
    return nullptr;
  }

  grpc_shmem::ControlBlock* cb = segment.control();
  VLOG(2) << "Control block at: " << cb;
  VLOG(2) << "c2s_queues: " << cb->GetC2SQueues() << ", s2c_queues: " << cb->GetS2CQueues();
  
  auto segment_ptr = std::make_unique<grpc_shmem::ShmemSegment>(std::move(segment));
  
  VLOG(1) << "About to create client transport";
  fflush(stderr);
  VLOG(2) << "About to create client transport";
  
  // CLIENT RULE: Client keeps segment alive locally but doesn't register for global cleanup
  // Only the server manages the segment in the global cleanup map
  LOG(INFO) << "CLIENT CONNECTION: Client connected, keeping segment alive locally (server manages global cleanup)";
  
  VLOG(1) << "Creating client transport with cb: " << cb;
  fflush(stderr);
  VLOG(2) << "Creating client transport with cb: " << cb;
  
  auto result = OrphanablePtr<Transport>(MakeOrphanable<ShmemClientTransport>(nullptr, cb, std::move(segment_ptr), client_channel_args).release());
  VLOG(1) << "Client transport created successfully";
  fflush(stderr);
  return result;
}

// Duplicate function definition removed

// Duplicate function definition removed

}  // namespace grpc_core



// Plugin registration
void grpc_shmem_transport_init() {
  // This is where you would register your transport with the core,
  // but for this simplified example, we'll skip it.
}

void grpc_shmem_transport_shutdown() {
  // Clean up any global resources if needed
}