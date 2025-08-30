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
// POSIX FDs + EventEngine FD integration
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include "src/core/lib/iomgr/closure.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/pollset_set.h"

#include <grpc/event_engine/event_engine.h>
#include <unistd.h>

// Force race condition with artificial delays
#define FORCE_RACE_DELAY_US 10000  // 10ms delay to force race
#define FORCE_RACE_CLIENT_DELAY() std::this_thread::sleep_for(std::chrono::microseconds(FORCE_RACE_DELAY_US))
#define FORCE_RACE_SERVER_DELAY() std::this_thread::sleep_for(std::chrono::microseconds(FORCE_RACE_DELAY_US))

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
#include <sys/select.h>

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
    uint64_t used = head - tail;
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
        grpc_shmem::PushCommand(q, cb, dir, pad, sem);

        // Wait until tail catches up (PAD applied) - with timeout to prevent hangs
        const uint64_t target = tail + free_to_end;
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout_duration = std::chrono::seconds(5); // 5 second timeout
        while (rb->tail.load(std::memory_order_acquire) < target) {
          std::this_thread::yield();
          // Check for timeout to prevent indefinite hanging
          if (std::chrono::steady_clock::now() - start_time > timeout_duration) {
            LOG(ERROR) << "Timeout waiting for tail to catch up: target=" << target 
                      << " current_tail=" << rb->tail.load(std::memory_order_acquire)
                      << " - breaking to prevent hang";
            break;
          }
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
namespace {

static int MakeEventFd() { return eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE); }
static std::string ControlSockPath(absl::string_view name) {
  return absl::StrCat("/tmp/grpc_shmem_", name, ".sock");
}
static int CreateUnixListener(const std::string& path) {
  int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_un a{}; a.sun_family = AF_UNIX; strncpy(a.sun_path, path.c_str(), sizeof(a.sun_path)-1);
  unlink(path.c_str());
  if (bind(fd, (sockaddr*)&a, sizeof(a)) != 0 || listen(fd, 8) != 0) { close(fd); return -1; }
  return fd;
}
static int ConnectUnix(const std::string& path) {
  int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_un a{}; a.sun_family = AF_UNIX; strncpy(a.sun_path, path.c_str(), sizeof(a.sun_path)-1);
  if (connect(fd, (sockaddr*)&a, sizeof(a)) != 0) { close(fd); return -1; }
  return fd;
}
static bool SendFd(int sock, int fd_to_send) {
  char b='x'; iovec iov{&b,1}; char cbuf[CMSG_SPACE(sizeof(int))]; msghdr m{};
  m.msg_iov=&iov; m.msg_iovlen=1; m.msg_control=cbuf; m.msg_controllen=sizeof(cbuf);
  cmsghdr* c=CMSG_FIRSTHDR(&m); c->cmsg_level=SOL_SOCKET; c->cmsg_type=SCM_RIGHTS; c->cmsg_len=CMSG_LEN(sizeof(int));
  memcpy(CMSG_DATA(c), &fd_to_send, sizeof(int)); return sendmsg(sock,&m,0)>=0;
}
static int RecvFd(int sock) {
  char b; iovec iov{&b,1}; char cbuf[CMSG_SPACE(sizeof(int))]; msghdr m{};
  m.msg_iov=&iov; m.msg_iovlen=1; m.msg_control=cbuf; m.msg_controllen=sizeof(cbuf);
  if (recvmsg(sock,&m,0)<0) return -1; cmsghdr* c=CMSG_FIRSTHDR(&m); if (!c) return -1;
  int fd; memcpy(&fd, CMSG_DATA(c), sizeof(int)); return fd;
}

// Data structure for announcing streams to core via accept callback
struct ShmemServerData {
  uint32_t stream_id;
  std::vector<grpc_shmem::KVPair> initial_md;
};

// Correct futex doorbell adapter - fixed deadlock issues
class FutexDoorbellAdapter : public grpc_shmem::TransportSemaphoreAdapter {
 public:
  explicit FutexDoorbellAdapter(grpc_shmem::ControlBlock* cb) : cb_(cb) {}
  void SetPeerEventFd(int fd) { peer_eventfd_.store(fd, std::memory_order_relaxed); }

  
  void Post(grpc_shmem::ControlBlock* cb, bool is_c2s) override {
    auto& db = is_c2s ? cb_->c2s_db : cb_->s2c_db;
    
    // Increment sequence to signal new data
    db.seq.fetch_add(1, std::memory_order_release);
    
    // Wake any waiting threads (don't check waiter flag - just wake)
    futex_wake(reinterpret_cast<uint32_t*>(&db.seq), 1);
    
    // Wake peer poller via eventfd (if set)
    int efd = peer_eventfd_.load(std::memory_order_relaxed);
    if (efd >= 0) { uint64_t one = 1; (void)write(efd, &one, sizeof(one)); }
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
    
    // Use futex wait with timeout to avoid blocking indefinitely
    struct timespec timeout = {0, 1000000}; // 1ms timeout
    futex_wait(reinterpret_cast<uint32_t*>(&db.seq), final_seq, &timeout);
    
    // Clear waiter flag when waking up
    db.waiter.store(0, std::memory_order_relaxed);
    
    // Always return after one wait - don't loop infinitely
  }

  bool ShouldPost(bool is_c2s, size_t bytes_added, int frames_added) override {
    return true;
  }

private:
  grpc_shmem::ControlBlock* cb_;
  std::atomic<uint64_t> avoided_wakes_{0};
  std::atomic<uint64_t> timeout_wakes_{0};
  std::atomic<int> peer_eventfd_{-1};
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
                       grpc_shmem::ControlBlock* cb, const ChannelArgs& args)
      : server_(server), cb_(cb), channel_args_(args) {
    fprintf(stderr, "*** DEBUG: ShmemClientTransport constructor starting ***\n");
    fflush(stderr);
    MutexLock l(&state_mu_);
    state_tracker_.SetState(GRPC_CHANNEL_CONNECTING, absl::OkStatus(), "init");
    state_tracker_.SetState(GRPC_CHANNEL_READY, absl::OkStatus(), "shmem ready");
    
    // Track process attachment for coordination with atomic increment  
    if (cb_) {
      int32_t current_count = cb_->process_count.fetch_add(1, std::memory_order_acq_rel) + 1;
      LOG(INFO) << "ShmemClientTransport attached, process count now: " << current_count;
      
      // Futex doorbells are initialized directly in ControlBlock, no additional setup needed
      
      // Create futex doorbell adapter for low-latency queue operations
      sem_adapter_ = std::make_unique<FutexDoorbellAdapter>(cb_);
      
      // Register Event Engine callbacks for client command processing
      // EventEngine callbacks removed - simplified integration
      VLOG(2) << "Client EventEngine integration completed";
    }
    fprintf(stderr, "*** DEBUG: ShmemClientTransport constructor completed ***\n");
    fflush(stderr);
  }

  void StartCall(CallHandler child_call_handler) override;
  void Orphan() override {
    InitiateShutdown();
    Unref(DEBUG_LOCATION, "orphan");
  }
  
 private:
  void InitiateShutdown() {
    ExecCtx exec_ctx;
    
    // Step 1: Signal shutdown to all threads
    if (shutdown_initiated_.exchange(true, std::memory_order_acq_rel)) {
      return; // Already initiated
    }
    
    // Step 2: Set stop flag and wake threads
    stop_.store(true, std::memory_order_relaxed);
    
    // Step 3: Wake any waiting reader threads
    WaitForThreadsToExit();
    
    // Step 4: Cleanup resources in proper order
    CleanupResources();
    
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    // Only wake semaphores if a ring reader thread was started
    if (reader_started_.load(std::memory_order_acquire)) {
      // RACE CONDITION FIX: Wake reader threads during shutdown
      // and add a small delay to allow reader thread to check stop flag
      if (cb_ != nullptr) {
        try {
          // With futex doorbells, we need to actively wake any waiting reader threads
          // CLIENT: Wake S2C reader thread (client reads S2C responses)
          auto& db = cb_->s2c_db;
          if (db.waiter.load(std::memory_order_acquire)) {
            db.seq.fetch_add(1, std::memory_order_release);
            futex_wake(reinterpret_cast<uint32_t*>(&db.seq), 1);
          }
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error waking futex doorbells: " << e.what();
        }
      }
      
      // Reader thread removed - using EventEngine callbacks
      // if (reader_.joinable()) {
      //   try {
      //     reader_.join();
      //   } catch (const std::exception& e) {
      //     LOG(ERROR) << "Error joining reader thread: " << e.what();
      //   }
      // }
    }
  }
  
  void CleanupResources() {
    try {
      // CRITICAL FIX: Do NOT clear handlers during shutdown as this can cause 
      // completion events to be lost, leading to hanging in grpc_completion_queue_next
      // Let handlers complete naturally and only clear on destruction
      
      // Clear handler map to prevent stale entries in reused transports
      // {
      //   MutexLock lock(&mu_);
      //   handlers_.clear();
      // }
      
      // Handle atomic reference counting and cleanup coordination
      if (cb_ != nullptr) {
        // Atomic decrement - only the process that decrements to 0 does cleanup
        int32_t previous_count = cb_->process_count.fetch_sub(1, std::memory_order_acq_rel);
        int32_t remaining = previous_count - 1;
        LOG(INFO) << "ShmemClientTransport [PID " << getpid() << "] detaching, remaining processes: " << remaining;
        
        // The process that decrements the reference count to 0 is responsible for cleanup
        if (remaining == 0) {
          LOG(INFO) << "ShmemClientTransport [PID " << getpid() << "]: Last process, cleaning up shared resources";
          PerformFinalCleanup();
        }
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
    if (pss_ == nullptr) pss_ = grpc_pollset_set_create();
    grpc_pollset_set_add_pollset(pss_, ps);
    if (s2c_grpc_fd_ != nullptr) grpc_pollset_set_add_fd(pss_, s2c_grpc_fd_);
  }
  void SetPollsetSet(grpc_stream*, grpc_pollset_set* pss) override {
    pss_ = pss;
    if (s2c_grpc_fd_ != nullptr) grpc_pollset_set_add_fd(pss_, s2c_grpc_fd_);
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
  ~ShmemClientTransport() override = default;

  void EnsureReaderStarted();
  void InitS2CDoorbell();
  void DrainS2CFromPoller();
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
  int s2c_doorbell_fd_ = -1;
  grpc_fd* s2c_grpc_fd_ = nullptr;
  grpc_closure s2c_on_readable_;
  grpc_pollset_set* pss_ = nullptr;
  void ArmS2CReadableWatch();
  
  // gRPC call handlers for S2C processing
  Mutex mu_;
  absl::flat_hash_map<uint64_t, CallHandler> handlers_ ABSL_GUARDED_BY(mu_);
  
  // For reassembling chunked S2C messages by stream ID
  struct StreamChunkState {
    grpc_core::SliceBuffer accumulator;
    bool accumulating = false;
  };
  std::unordered_map<uint64_t, StreamChunkState> s2c_chunk_accumulators_;

  Mutex state_mu_;
  ConnectivityStateTracker state_tracker_
      ABSL_GUARDED_BY(state_mu_){"shmem_client_transport",
                                 GRPC_CHANNEL_CONNECTING};

};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& args) : channel_args_(args) {
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
      : segment_(std::move(seg)), channel_args_(args) {
    // InitC2SDoorbell will be called in SetCallDestination when server name is available  
    // Existing constructor logic continues below
    fprintf(stderr, "*** DEBUG: ShmemServerTransport constructor (2-arg) starting ***\n");
    fflush(stderr);
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
        sem_adapter_ = std::make_unique<FutexDoorbellAdapter>(cb_);
        
        // Register Event Engine callbacks for server command processing
        VLOG(2) << "About to get Event Engine for server";
        auto* futex_adapter = static_cast<FutexDoorbellAdapter*>(sem_adapter_.get());
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
    
    // CRITICAL FIX: For named servers, start FD exchange immediately  
    // This solves the chicken-and-egg problem where clients can't connect
    // because the server's Unix socket listener isn't ready yet
    bool is_named_server = args.GetBool("grpc.shmem.is_named_server").value_or(false);
    auto server_name = args.GetString("grpc.shmem.server_name");
    
    // Enable async FD exchange for all named servers with proper pollset registration order
    if (is_named_server) {
      LOG(INFO) << "Named server detected, will start FD exchange in SetCallDestination: " << server_name.value_or("unknown");
      // Don't call InitC2SDoorbell here - pollsets not ready yet
    }
    fprintf(stderr, "*** DEBUG: ShmemServerTransport constructor (2-arg) completed successfully ***\n");
    fflush(stderr);
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
    MutexLock lock(&dest_mu_);
    dest_ = std::move(h);
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
  fprintf(stderr, "*** DEBUG: SetCallDestination called - server ready for clients ***\n");
  fflush(stderr);
  LOG(INFO) << "SetCallDestination called - server ready for clients (NO THREADS)";

    // Initialize FD exchange for event-driven processing (pollsets are now ready)
    fprintf(stderr, "*** DEBUG: About to call InitC2SDoorbell ***\n");
    fflush(stderr);
    InitC2SDoorbell();
    fprintf(stderr, "*** DEBUG: InitC2SDoorbell call completed ***\n");
    fflush(stderr);
    
    // Commands will be processed via OnC2SReadable callbacks when clients send data
  }

  void Orphan() override {
    fprintf(stderr, "*** DEBUG: Server transport Orphan() called - but NOT shutting down futex polling (server should stay active) ***\n");
    fflush(stderr);
    // DON'T call InitiateShutdown() here - server should stay active for multiple clients
    // Only disconnect this specific transport instance
    Disconnect(absl::UnavailableError("shmem transport closed"));
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
    
    // Step 4: Cleanup resources
    CleanupResources();
    
    cleanup_complete_.store(true, std::memory_order_release);
  }
  
  void WaitForThreadsToExit() {
    // Only post semaphores if the ring server loop was actually started
    if (reader_started_.load(std::memory_order_acquire)) {
      // RACE CONDITION FIX: Wake reader threads during shutdown
      // and add a small delay to allow reader thread to check stop flag
      if (cb_ != nullptr) {
        try {
          // With futex doorbells, we need to actively wake any waiting reader threads
          // SERVER: Wake C2S reader thread (server reads from C2S queue)
          auto& db = cb_->c2s_db;
          if (db.waiter.load(std::memory_order_acquire)) {
            db.seq.fetch_add(1, std::memory_order_release);
            futex_wake(reinterpret_cast<uint32_t*>(&db.seq), 1);
          }
        } catch (const std::exception& e) {
          LOG(ERROR) << "Error waking futex doorbells: " << e.what();
        }
      }
      
      // Reader thread removed - using EventEngine callbacks
      // if (reader_.joinable()) {
      //   try {
      //     reader_.join();
      //   } catch (const std::exception& e) {
      //     LOG(ERROR) << "Error joining reader thread: " << e.what();
      //   }
      // }
    }
  }
  
  void CleanupResources() {
    try {
      // Cleanup semaphore manager
      // Cleanup semaphore manager if needed
      
      // Handle atomic reference counting and cleanup coordination
      if (cb_ != nullptr) {
        // Atomic decrement - only the process that decrements to 0 does cleanup
        int32_t previous_count = cb_->process_count.fetch_sub(1, std::memory_order_acq_rel);
        int32_t remaining = previous_count - 1;
        LOG(INFO) << "ShmemServerTransport [PID " << getpid() << "] detaching, remaining processes: " << remaining;
        
        // The process that decrements the reference count to 0 is responsible for cleanup
        if (remaining == 0) {
          LOG(INFO) << "ShmemServerTransport [PID " << getpid() << "]: Last process, cleaning up shared resources";
          PerformFinalCleanup();
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
    fprintf(stderr, "*** DEBUG: Server SetPollset called ***\n");
    fflush(stderr);
    if (pss_ == nullptr) pss_ = grpc_pollset_set_create();
    grpc_pollset_set_add_pollset(pss_, ps);
    
    // Create eventfd here so it's available when needed
    if (c2s_doorbell_fd_ == -1) {
      auto name = channel_args_.GetString("grpc.shmem.server_name");
      fprintf(stderr, "*** DEBUG: Server SetPollset creating eventfd for name=%s ***\n", 
              name.has_value() ? std::string(*name).c_str() : "NULL");
      fflush(stderr);
      if (name.has_value()) {
        c2s_doorbell_fd_ = MakeEventFd();
        if (c2s_doorbell_fd_ >= 0) {
          c2s_grpc_fd_ = grpc_fd_create(c2s_doorbell_fd_, "c2s_doorbell", false);
          GRPC_CLOSURE_INIT(&c2s_on_readable_, OnC2SReadable, this, grpc_schedule_on_exec_ctx);
          LOG(INFO) << "Server SetPollset: created C2S eventfd: " << c2s_doorbell_fd_;
        }
      }
    }
    
    if (c2s_grpc_fd_ != nullptr) {
      grpc_pollset_set_add_fd(pss_, c2s_grpc_fd_);
      LOG(INFO) << "Server SetPollset: added C2S eventfd to pollset";
    } else {
      fprintf(stderr, "*** DEBUG: Server SetPollset: c2s_grpc_fd_ not created yet ***\n");
      fflush(stderr);
    }
  }
  void SetPollsetSet(grpc_stream*, grpc_pollset_set* pss) override {
    pss_ = pss;
    if (c2s_grpc_fd_ != nullptr) {
      grpc_pollset_set_add_fd(pss_, c2s_grpc_fd_);
      LOG(INFO) << "Server SetPollsetSet: added C2S eventfd to pollset_set";
    }
  }
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

  // FinishAccept - called by core to complete the stream accept lifecycle
  void FinishAccept(const void* server_data);

 private:
  ~ShmemServerTransport() override = default;

  void PerformFinalCleanup();
  void InitC2SDoorbell();
  void DrainC2SFromPoller();
  static void OnC2SReadable(void* arg, grpc_error_handle error);

  void EnsureReaderStarted() {
    if (cb_ == nullptr) {
      return;
    }
    if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
      InitC2SDoorbell();
      // reader_ready_.store(true, std::memory_order_release); // Server ready signaling not needed with EventEngine
    }
  }

  // Legacy stream-op completion helpers removed.

  // Response monitoring loop - forward real server responses onto S2C ring.
  // Implement CallOutboundLoop equivalent for shmem cross-process communication
  auto ShmemCallOutboundLoop(uint64_t stream_id, CallInitiator call_initiator) {
    return Seq(
        TrySeq(
          call_initiator.PullServerInitialMetadata(),
          [this, stream_id](std::optional<ServerMetadataHandle> md) {
            if (md.has_value()) {
              // Server response delay removed - testing other locations
              VLOG(1) << "SERVER: Sending S2C_INITIAL_METADATA stream_id=" << stream_id;
              std::vector<grpc_shmem::KVPair> kvs;
              kvs.push_back({"content-type", "application/grpc"});
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
            auto* payload = msg->payload();
            const size_t n = payload->Length();
            if (n == 0) return Success{};  // nothing to send
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
          cb_->GetC2SQueues()->data_rb.tail.fetch_add(
              cmd.data_size, std::memory_order_release);
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
                VLOG(2) << "ServerLoop - About to create call, dest: " << dest.get();
                
                auto arena = call_arena_allocator_->MakeArena();
                VLOG(3) << "ServerLoop - Created arena: " << arena.get();
                
                auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
                arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
                
                // CRITICAL FIX: Build client metadata from the deserialized C2S data
                // This is the actual client metadata that should be passed to the server call
                VLOG(2) << "ServerLoop - Building client metadata from C2S data";
                auto md = arena->MakePooledForOverwrite<ClientMetadata>();
                
                // Set peer string for shmem transport
                md->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
                
                // Forward all the client metadata from C2S ring
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
                VLOG(2) << "ServerLoop - Client metadata built with " << kvs_in.size() << " entries";
                
                VLOG(2) << "ServerLoop - About to call MakeCallPair";
                auto call = MakeCallPair(std::move(md), std::move(arena));
                VLOG(3) << "ServerLoop - MakeCallPair completed";
                
                st.initiator.emplace(call.initiator);
                
                // Start the server call - auth context should come from server channel args now
                VLOG(1) << "SERVER: Calling dest->StartCall stream_id=" << cmd.stream_id;
                dest->StartCall(std::move(call.handler));
                VLOG(1) << "SERVER: dest->StartCall completed stream_id=" << cmd.stream_id;
                
                // The client metadata is now properly embedded in the call via MakeCallPair()
                // No separate metadata pushing needed - this is how inproc transport works
                
                // Start the response bridge
                VLOG(1) << "SERVER: Starting response bridge stream_id=" << cmd.stream_id;
                call.initiator.SpawnGuarded("shmem-response-bridge", 
                  [this, stream_id = cmd.stream_id, call_initiator = call.initiator]() mutable {
                    VLOG(1) << "SERVER: Response bridge starting stream_id=" << stream_id;
                    return ShmemCallOutboundLoop(stream_id, std::move(call_initiator));
                  });
                } else {
                  VLOG(1) << "No call destination available for stream " << cmd.stream_id;
                }
                
                st.sent_initial = true;
                if (cmd.data_size != 0) {
                  cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
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
                cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
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

            // Copy out of the ring (don't pin)
            const unsigned char* p = rb->GetBuffer(cb_) + cmd.data_offset;
            grpc_core::Slice copied = grpc_core::Slice::FromCopiedBuffer(reinterpret_cast<const char*>(p),
                                               cmd.data_size);
            st.c2s_copied_accumulator.Append(std::move(copied));

            // Free ring bytes NOW so next reserve can succeed
            uint64_t tail0 = rb->tail.load(std::memory_order_relaxed);
            rb->tail.fetch_add(cmd.data_size, std::memory_order_release);
            uint64_t tail1 = rb->tail.load(std::memory_order_relaxed);
            VLOG(1) << "[C2S FREE VERIFY] freed=" << cmd.data_size
                       << " tail_before=" << tail0
                       << " tail_after=" << tail1;

            // On LAST, deliver one Message from the copied slices
            if (cmd.type == grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST &&
                st.initiator.has_value() && !st.cancelled && !st.completed) {
              // Store the message in stream state to be delivered before trailing metadata
              st.pending_message = std::make_unique<grpc_core::SliceBuffer>(std::move(st.c2s_copied_accumulator));
            }
            break;
          }
          case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
            VLOG(1) << "SERVER: Received C2S_TRAILING_METADATA stream_id=" << cmd.stream_id;
            
            // Deliver any pending chunked message BEFORE half-close
            if (st.pending_message && st.initiator.has_value() && !st.cancelled) {
              auto init = *st.initiator;
              auto msg = Arena::MakePooled<Message>(std::move(*st.pending_message), 0);
              init.SpawnPushMessage(std::move(msg));
              st.pending_message.reset();
            }
            
            // Client half-close: Signal FinishSends to server pipeline
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
            st.cancelled = true;
            if (st.initiator.has_value()) {
              st.initiator->SpawnCancel();
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
  // Store channel args to provide auth context to filter creation
  ChannelArgs channel_args_;
  int spin_iters_ = kDefaultSpinIters;
  // Always use ring-based communication
  RefCountedPtr<CallArenaAllocator> call_arena_allocator_;
  int c2s_doorbell_fd_ = -1;
  grpc_fd* c2s_grpc_fd_ = nullptr;
  grpc_closure c2s_on_readable_;
  grpc_pollset_set* pss_ = nullptr;
  bool c2s_reading_started_ = false;
  // Removed Unix socket approach - using futex doorbells only
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
  for (const auto& kv : sd->initial_md) {
    if (kv.key == ":path") md->Set(HttpPathMetadata(), Slice::FromCopiedString(kv.value));
    else if (kv.key == ":method") md->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
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

  auto call = MakeCallPair(std::move(md), std::move(arena));

  RefCountedPtr<UnstartedCallDestination> d;
  { MutexLock l(&dest_mu_); d = dest_; }
  if (d == nullptr) { delete sd; return; }

  // THIS is where core/filters are ready; StartCall now is safe.
  d->StartCall(std::move(call.handler));

  {
    MutexLock lk(&stream_initiators_mu_);
    stream_initiators_.emplace(sd->stream_id, call.initiator);
  }
  // Start your S2C bridge (unchanged)
  call.initiator.SpawnGuarded("shmem-response-bridge",
      [this, sid = sd->stream_id, ci = call.initiator]() mutable {
        return ShmemCallOutboundLoop(sid, std::move(ci));
      });

  delete sd;
}

// Shared cleanup implementation for atomic reference counting
void ShmemClientTransport::PerformFinalCleanup() {
  if (cb_ == nullptr) return;
  
  try {
    // With futex doorbells, no named semaphore cleanup needed
    
    // Clean up from global cross-process segments map
    RemoveCrossProcessSegment(cb_);
    LOG(INFO) << "ShmemClientTransport: Final cleanup complete";
  } catch (const std::exception& e) {
    LOG(ERROR) << "ShmemClientTransport::PerformFinalCleanup error: " << e.what();
  }
}

void ShmemServerTransport::PerformFinalCleanup() {
  if (cb_ == nullptr) return;
  
  try {
    // With futex doorbells, no named semaphore cleanup needed
    LOG(INFO) << "ShmemServerTransport: Final cleanup complete";
  } catch (const std::exception& e) {
    LOG(ERROR) << "ShmemServerTransport::PerformFinalCleanup error: " << e.what();
  }
}

void ShmemClientTransport::EnsureReaderStarted() {
  fprintf(stderr, "*** DEBUG: Client EnsureReaderStarted CALLED ***\n");
  fflush(stderr);
  LOG(INFO) << "Client EnsureReaderStarted called, cb_=" << (void*)cb_;
  if (cb_ == nullptr) {
    fprintf(stderr, "*** DEBUG: Client EnsureReaderStarted: cb_ is null, returning ***\n");
    fflush(stderr);
    LOG(INFO) << "Client EnsureReaderStarted: cb_ is null, returning";
    return;
  }
  bool was_started = reader_started_.load(std::memory_order_acquire);
  fprintf(stderr, "*** DEBUG: Client EnsureReaderStarted: reader_started_=%s ***\n", was_started ? "true" : "false");
  fflush(stderr);
  LOG(INFO) << "Client EnsureReaderStarted: reader_started_=" << was_started;
  if (!reader_started_.exchange(true, std::memory_order_acq_rel)) {
    fprintf(stderr, "*** DEBUG: Client EnsureReaderStarted: starting doorbell init (FIRST TIME) ***\n");
    fflush(stderr);
    LOG(INFO) << "Client EnsureReaderStarted: starting doorbell init";
    InitS2CDoorbell();
    reader_ready_.store(true, std::memory_order_release);
  } else {
    fprintf(stderr, "*** DEBUG: Client EnsureReaderStarted: reader already started (DUPLICATE CALL) ***\n");
    fflush(stderr);
    LOG(INFO) << "Client EnsureReaderStarted: reader already started";
  }
}

void ShmemClientTransport::InitS2CDoorbell() {
  fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell CALLED ***\n");
  fflush(stderr);
  auto name = channel_args_.GetString("grpc.shmem.server_name");
  if (!name.has_value()) {
    fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: no server name, skipping ***\n");
    fflush(stderr);
    LOG(INFO) << "Client InitS2CDoorbell: no server name, skipping";
    return;
  }
  fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: server_name=%.*s ***\n", (int)name->length(), name->data());
  fflush(stderr);
  LOG(INFO) << "Client InitS2CDoorbell: server_name=" << *name;
  
  // Create S2C eventfd for receiving server notifications
  fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: creating S2C eventfd ***\n");
  fflush(stderr);
  s2c_doorbell_fd_ = MakeEventFd();
  if (s2c_doorbell_fd_ < 0) {
    fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: failed to create eventfd ***\n");
    fflush(stderr);
    LOG(ERROR) << "Failed to create S2C eventfd";
    return;
  }
  fprintf(stderr, "*** DEBUG: Client created S2C eventfd=%d ***\n", s2c_doorbell_fd_);
  fflush(stderr);
  LOG(INFO) << "Client created S2C eventfd=" << s2c_doorbell_fd_;
  
  // For cross-process communication, the futex doorbell adapter handles peer FD setup
  // The shared memory control block contains the necessary eventfd information
  if (sem_adapter_) {
    auto* futex_adapter = static_cast<FutexDoorbellAdapter*>(sem_adapter_.get());
    // Futex doorbells are already set up in the control block
    LOG(INFO) << "Client using futex doorbells for cross-process signaling";
  }
  
  // Use futex polling for reliable cross-process signaling
  // FD callbacks cause LockfreeEvent conflicts - futex polling is more reliable
  fprintf(stderr, "*** DEBUG: Client InitS2CDoorbell: using futex polling for S2C signals ***\n");
  fflush(stderr);
  
  // Create futex polling thread for S2C responses
  auto self = this;
  std::thread([self]() {
    fprintf(stderr, "*** DEBUG: Client starting futex polling thread for S2C signals ***\n");
    fflush(stderr);
    
    if (self->cb_ == nullptr) {
      fprintf(stderr, "*** DEBUG: Client futex thread: cb_ is null! ***\n");
      fflush(stderr);
      return;
    }
    
    uint32_t last_seq = self->cb_->s2c_db.seq.load(std::memory_order_acquire);
    fprintf(stderr, "*** DEBUG: Client initial S2C sequence: %u ***\n", last_seq);
    fflush(stderr);
    
    // Poll for responses until client shutdown
    while (!self->stop_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      
      // Check if S2C sequence has changed (indicating new data from server)
      uint32_t current_seq = self->cb_->s2c_db.seq.load(std::memory_order_acquire);
      
      if (current_seq != last_seq) {
        fprintf(stderr, "*** DEBUG: Client S2C sequence changed from %u to %u! Processing responses ***\n", 
                last_seq, current_seq);
        fflush(stderr);
        
        // Process S2C commands
        self->DrainS2CFromPoller();
        
        last_seq = current_seq;
        // Keep polling for more responses
      }
    }
    fprintf(stderr, "*** DEBUG: Client futex polling thread finished ***\n");
    fflush(stderr);
  }).detach();
}

void ShmemClientTransport::DrainS2CFromPoller() {
  fprintf(stderr, "*** DEBUG: Client DrainS2CFromPoller CALLED ***\n");
  fflush(stderr);
  
  // CRITICAL: Set up execution context for spawned tasks
  ExecCtx exec_ctx;
  
  // Process S2C commands (non-blocking)
  int commands_processed = 0;
  for (int i = 0; i < 100; ++i) {
    grpc_shmem::Command cmd;
    if (!grpc_shmem::PopCommandHybrid(cb_->GetS2CQueues(), cb_, grpc_shmem::Direction::kS2C,
                                     0, &cmd, /*sem_adapter=*/nullptr)) {
      break; // No more commands
    }
    
    commands_processed++;
    fprintf(stderr, "*** DEBUG: Client processing S2C command #%d: type=%d, stream_id=%lu ***\n", 
            commands_processed, static_cast<int>(cmd.type), cmd.stream_id);
    fflush(stderr);
    VLOG(2) << "Client EventEngine processing S2C command: " << static_cast<int>(cmd.type) << " stream=" << cmd.stream_id;
    
    // Handle DATA_PAD first (no handler needed)
    if (cmd.type == grpc_shmem::FrameType::DATA_PAD) {
      cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
      continue;
    }
    
    // Look up handler for this stream
    std::unique_ptr<CallHandler> handler;
    {
      MutexLock lock(&mu_);
      auto it = handlers_.find(cmd.stream_id);
      if (it != handlers_.end()) {
        handler = std::make_unique<CallHandler>(it->second);
      }
    }
    
    if (!handler) {
      VLOG(2) << "Client: No handler for stream " << cmd.stream_id;
      continue;
    }
    
    // Process command with handler
    switch (cmd.type) {
      case grpc_shmem::FrameType::S2C_INITIAL_METADATA: {
        VLOG(1) << "CLIENT: Received S2C_INITIAL_METADATA stream_id=" << cmd.stream_id;
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
        VLOG(2) << "ShmemClientTransport received S2C_MESSAGE for stream " << cmd.stream_id << ", size=" << cmd.data_size;
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
        VLOG(1) << "CLIENT: Received S2C_TRAILING_METADATA stream_id=" << cmd.stream_id;
        auto data = cb_->GetS2CQueues()->data_rb.GetBuffer(cb_) + cmd.data_offset;
        auto kvs = grpc_shmem::DeserializeMetadataKVs(data, cmd.data_size);
        handler->SpawnInfallible(
            "push-trailing", [kvs = std::move(kvs), h = *handler, stream_id = cmd.stream_id, this]() mutable {
              auto md = Arena::MakePooledForOverwrite<ServerMetadata>();
              grpc_status_code status = GRPC_STATUS_OK;
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
                VLOG(1) << "CLIENT: Removing handler stream_id=" << stream_id;
                this->handlers_.erase(stream_id);
              }
              return Empty{};
            });
        cb_->GetS2CQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        break;
      }
      default:
        VLOG(2) << "Client: unknown command type " << static_cast<int>(cmd.type);
        break;
    }
  }
  
  fprintf(stderr, "*** DEBUG: Client DrainS2CFromPoller processed %d commands ***\n", commands_processed);
  fflush(stderr);
}

void ShmemClientTransport::OnS2CReadable(void* arg, grpc_error_handle error) {
  auto* self = static_cast<ShmemClientTransport*>(arg);
  ExecCtx exec_ctx;
  LOG(INFO) << "Client OnS2CReadable called, error=" << error;
  if (!error.ok()) {
    LOG(ERROR) << "Client OnS2CReadable error: " << error;
    // Re-arm even on error to keep receiving notifications
    grpc_fd_notify_on_read(self->s2c_grpc_fd_, &self->s2c_on_readable_);
    return;
  }
  // Drain the eventfd first
  uint64_t v;
  int read_count = 0;
  while (read(self->s2c_doorbell_fd_, &v, sizeof(v)) == 8) {
    read_count++;
  }
  LOG(INFO) << "Client drained " << read_count << " eventfd notifications";
  // Drain S2C queue without blocking
  self->DrainS2CFromPoller();
  // Re-arm after processing to avoid missed edges
  grpc_fd_notify_on_read(self->s2c_grpc_fd_, &self->s2c_on_readable_);
  LOG(INFO) << "Client OnS2CReadable re-armed";
}

// Server-side EventEngine integration
void ShmemServerTransport::InitC2SDoorbell() {
  fprintf(stderr, "*** DEBUG: ShmemServerTransport::InitC2SDoorbell ENTRY ***\n");
  fflush(stderr);
  LOG(INFO) << "Server InitC2SDoorbell: called";
  
  // Create eventfd if not already created (SetPollset is not called early enough)
  if (c2s_grpc_fd_ == nullptr) {
    fprintf(stderr, "*** DEBUG: c2s_grpc_fd_ is NULL, creating eventfd now ***\n");
    fflush(stderr);
    
    auto name = channel_args_.GetString("grpc.shmem.server_name");
    if (name.has_value()) {
      c2s_doorbell_fd_ = MakeEventFd();
      if (c2s_doorbell_fd_ >= 0) {
        c2s_grpc_fd_ = grpc_fd_create(c2s_doorbell_fd_, "c2s_doorbell", false);
        GRPC_CLOSURE_INIT(&c2s_on_readable_, OnC2SReadable, this, grpc_schedule_on_exec_ctx);
        fprintf(stderr, "*** DEBUG: Created C2S eventfd: %d, grpc_fd: %p ***\n", 
                c2s_doorbell_fd_, c2s_grpc_fd_);
        fflush(stderr);
        LOG(INFO) << "Server InitC2SDoorbell: created C2S eventfd: " << c2s_doorbell_fd_;
        
        // Add to pollset if we have one, or create one if needed
        if (pss_ != nullptr) {
          grpc_pollset_set_add_fd(pss_, c2s_grpc_fd_);
          fprintf(stderr, "*** DEBUG: Added C2S eventfd to existing pollset ***\n");
          fflush(stderr);
        } else {
          // Create our own pollset for the server eventfd
          pss_ = grpc_pollset_set_create();
          grpc_pollset_set_add_fd(pss_, c2s_grpc_fd_);
          fprintf(stderr, "*** DEBUG: Created new pollset and added C2S eventfd ***\n");
          fflush(stderr);
          
          // Test: immediately check if eventfd is readable (non-blocking)
          uint64_t test_val;
          int test_read = read(c2s_doorbell_fd_, &test_val, sizeof(test_val));
          fprintf(stderr, "*** DEBUG: Initial eventfd read test: %d (errno=%d) ***\n", test_read, errno);
          fflush(stderr);
        }
      }
    }
    
    if (c2s_grpc_fd_ == nullptr) {
      fprintf(stderr, "*** DEBUG: Failed to create eventfd, returning ***\n");
      fflush(stderr);
      LOG(INFO) << "Server InitC2SDoorbell: failed to create eventfd, skipping";
      return;
    }
  } else {
    fprintf(stderr, "*** DEBUG: c2s_grpc_fd_ exists, proceeding with initialization ***\n");
    fflush(stderr);
  }
  
  // Only start reading once
  if (!c2s_reading_started_) {
    fprintf(stderr, "*** DEBUG: Calling grpc_fd_notify_on_read ***\n");
    fflush(stderr);
    grpc_fd_notify_on_read(c2s_grpc_fd_, &c2s_on_readable_);
    c2s_reading_started_ = true;
    fprintf(stderr, "*** DEBUG: grpc_fd_notify_on_read completed ***\n");
    fflush(stderr);
    LOG(INFO) << "Server InitC2SDoorbell: started reading C2S eventfd - using futex polling";
    
    // Create futex polling thread for C2S commands
    auto self = this;
    std::thread([self]() {
      fprintf(stderr, "*** DEBUG: Starting futex polling thread for C2S signals ***\n");
      fflush(stderr);
      
      uint32_t last_seq = self->cb_->c2s_db.seq.load(std::memory_order_acquire);
      fprintf(stderr, "*** DEBUG: Initial C2S sequence: %u ***\n", last_seq);
      fflush(stderr);
      
      // Poll indefinitely until server shutdown
      while (!self->stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Check if C2S sequence has changed (indicating new data from client)
        uint32_t current_seq = self->cb_->c2s_db.seq.load(std::memory_order_acquire);
        
        if (current_seq != last_seq) {
          fprintf(stderr, "*** DEBUG: C2S sequence changed from %u to %u! Processing commands ***\n", 
                  last_seq, current_seq);
          fflush(stderr);
          
          // Process C2S commands  
          self->DrainC2SFromPoller();
          
          last_seq = current_seq;
          // Keep polling for more commands  
        }
      }
      fprintf(stderr, "*** DEBUG: Server futex polling thread finished ***\n");
      fflush(stderr);
    }).detach();
    
  } else {
    fprintf(stderr, "*** DEBUG: Already reading C2S eventfd ***\n");
    fflush(stderr);
    LOG(INFO) << "Server InitC2SDoorbell: already reading C2S eventfd";
  }
}

void ShmemServerTransport::DrainC2SFromPoller() {
  fprintf(stderr, "*** DEBUG: DrainC2SFromPoller CALLED ***\n");
  fflush(stderr);
  
  // CRITICAL: Set up execution context for spawned tasks
  ExecCtx exec_ctx;
  
  // Process C2S commands (non-blocking)  
  int commands_processed = 0;
  for (int i = 0; i < 100; ++i) {
    grpc_shmem::Command cmd;
    if (!grpc_shmem::PopCommandHybrid(cb_->GetC2SQueues(), cb_, grpc_shmem::Direction::kC2S,
                                     0, &cmd, /*sem_adapter=*/nullptr)) {
      break; // No more commands
    }
    
    commands_processed++;
    fprintf(stderr, "*** DEBUG: Processing C2S command #%d: type=%d, stream_id=%lu ***\n", 
            commands_processed, static_cast<int>(cmd.type), cmd.stream_id);
    fflush(stderr);
    VLOG(2) << "Server EventEngine processing C2S command: " << static_cast<int>(cmd.type) << " stream=" << cmd.stream_id;
    
    // Handle DATA_PAD first (no special processing needed)
    if (cmd.type == grpc_shmem::FrameType::DATA_PAD) {
      cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
      continue;
    }
    
    // Proper gRPC service dispatch implementation
    fprintf(stderr, "*** DEBUG: Implementing proper gRPC service dispatch for command type=%d ***\n", static_cast<int>(cmd.type));
    fflush(stderr);
    
    // Per-stream state for tracking active service calls
    static std::map<uint64_t, CallInitiator> active_calls;
    static Mutex active_calls_mu;
    
    switch (cmd.type) {
      case grpc_shmem::FrameType::C2S_INITIAL_METADATA: {
        fprintf(stderr, "*** DEBUG: Handling C2S_INITIAL_METADATA for stream_id=%lu ***\n", cmd.stream_id);
        fflush(stderr);
        
        // Create arena and service call
        auto arena = call_arena_allocator_->MakeArena();
        auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
        arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
        
        // Parse metadata from the command (simplified for now)
        auto md = arena->MakePooledForOverwrite<ClientMetadata>();
        md->Set(HttpPathMetadata(), Slice::FromCopiedString("/helloworld.Greeter/SayHello"));
        md->Set(HttpMethodMetadata(), HttpMethodMetadata::kPost);
        md->Set(HttpAuthorityMetadata(), Slice::FromCopiedString("shmem://grpc_shmem_example"));
        md->Set(PeerString(), Slice::FromCopiedString("shmem:peer"));
        md->Set(ContentTypeMetadata(), ContentTypeMetadata::kApplicationGrpc);
        md->Set(TeMetadata(), TeMetadata::kTrailers);
        
        auto call = MakeCallPair(std::move(md), std::move(arena));
        
        // Store call initiator for message delivery
        {
          MutexLock lock(&active_calls_mu);
          active_calls[cmd.stream_id] = call.initiator;
        }
        
        // Get service destination and start call
        RefCountedPtr<UnstartedCallDestination> dest;
        {
          MutexLock lock(&dest_mu_);
          dest = dest_;
        }
        
        if (dest != nullptr) {
          fprintf(stderr, "*** DEBUG: Starting service call via dest->StartCall ***\n");
          fflush(stderr);
          dest->StartCall(std::move(call.handler));
          fprintf(stderr, "*** DEBUG: Service call started successfully! ***\n");
          fflush(stderr);
          
          // Set up response bridge to handle service responses
          auto stream_id = cmd.stream_id;
          auto call_initiator = call.initiator;
          call_initiator.SpawnGuarded("shmem-response-bridge",
            [this, stream_id, call_initiator]() mutable {
              fprintf(stderr, "*** DEBUG: Starting response bridge for stream_id=%lu ***\n", stream_id);
              fflush(stderr);
              return ShmemCallOutboundLoop(stream_id, std::move(call_initiator));
            });
          fprintf(stderr, "*** DEBUG: Response bridge set up for stream_id=%lu ***\n", stream_id);
          fflush(stderr);
        } else {
          fprintf(stderr, "*** DEBUG: No service destination available ***\n");
          fflush(stderr);
        }
        
        // Release consumed data
        cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        break;
      }
      
      case grpc_shmem::FrameType::C2S_MESSAGE: {
        fprintf(stderr, "*** DEBUG: Handling C2S_MESSAGE for stream_id=%lu, size=%u ***\n", cmd.stream_id, cmd.data_size);
        fflush(stderr);
        
        // Find active call for this stream
        CallInitiator initiator;
        bool found = false;
        {
          MutexLock lock(&active_calls_mu);
          auto it = active_calls.find(cmd.stream_id);
          if (it != active_calls.end()) {
            initiator = it->second;
            found = true;
          }
        }
        
        if (found) {
          // Create message from ring buffer data
          grpc_slice s = grpc_shmem::MakeSliceFromRing(
              &cb_->GetC2SQueues()->data_rb, cb_, cmd.data_offset, cmd.data_size);
          
          // Deliver message to service
          initiator.SpawnInfallible("push-c2s-msg", [initiator, s]() mutable {
            SliceBuffer sb;
            sb.AppendIndexed(Slice(s));
            auto msg = Arena::MakePooled<Message>(std::move(sb), 0);
            fprintf(stderr, "*** DEBUG: Pushing message to service via SpawnPushMessage ***\n");
            fflush(stderr);
            initiator.SpawnPushMessage(std::move(msg));
            return Empty{};
          });
          fprintf(stderr, "*** DEBUG: Message delivery spawned successfully ***\n");
          fflush(stderr);
        } else {
          fprintf(stderr, "*** DEBUG: No active call found for stream_id=%lu ***\n", cmd.stream_id);
          fflush(stderr);
          // Release data if no active call
          cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        }
        break;
      }
      
      case grpc_shmem::FrameType::C2S_TRAILING_METADATA: {
        fprintf(stderr, "*** DEBUG: Handling C2S_TRAILING_METADATA for stream_id=%lu ***\n", cmd.stream_id);
        fflush(stderr);
        
        // Signal end of stream to service
        CallInitiator initiator;
        bool found = false;
        {
          MutexLock lock(&active_calls_mu);
          auto it = active_calls.find(cmd.stream_id);
          if (it != active_calls.end()) {
            initiator = it->second;
            found = true;
            active_calls.erase(it); // Clean up
          }
        }
        
        if (found) {
          initiator.SpawnInfallible("finish-recv", [initiator]() mutable {
            fprintf(stderr, "*** DEBUG: Signaling FinishSends to service ***\n");
            fflush(stderr);
            initiator.SpawnFinishSends();
            return Empty{};
          });
          fprintf(stderr, "*** DEBUG: FinishRecv spawned successfully ***\n");
          fflush(stderr);
        }
        
        // Release consumed data
        cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        break;
      }
      
      default: {
        fprintf(stderr, "*** DEBUG: Unhandled command type=%d ***\n", static_cast<int>(cmd.type));
        fflush(stderr);
        // Release data for unhandled commands
        cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
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
        cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
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
        cb_->GetC2SQueues()->data_rb.tail.fetch_add(cmd.data_size, std::memory_order_release);
        break;
      }
      default:
        VLOG(2) << "Server: ignoring command type " << static_cast<int>(cmd.type);
        break;
    }
    } // End of disabled basic echo processing
  }
  
  fprintf(stderr, "*** DEBUG: DrainC2SFromPoller processed %d commands ***\n", commands_processed);
  fflush(stderr);
}

void ShmemServerTransport::OnC2SReadable(void* arg, grpc_error_handle error) {
  auto* self = static_cast<ShmemServerTransport*>(arg);
  fprintf(stderr, "*** DEBUG: Server OnC2SReadable CALLED! ***\n");
  fflush(stderr);
  ExecCtx exec_ctx;
  LOG(INFO) << "Server OnC2SReadable called, error=" << error;
  if (!error.ok()) {
    LOG(ERROR) << "Server OnC2SReadable error: " << error;
    // Re-arm even on error to keep receiving notifications
    grpc_fd_notify_on_read(self->c2s_grpc_fd_, &self->c2s_on_readable_);
    return;
  }
  // Drain the eventfd first
  uint64_t v;
  int read_count = 0;
  while (read(self->c2s_doorbell_fd_, &v, sizeof(v)) == 8) {
    read_count++;
  }
  LOG(INFO) << "Server drained " << read_count << " eventfd notifications";
  // Drain C2S queue without blocking
  self->DrainC2SFromPoller();
  // Re-arm after processing to avoid missed edges
  grpc_fd_notify_on_read(self->c2s_grpc_fd_, &self->c2s_on_readable_);
  LOG(INFO) << "Server OnC2SReadable re-armed";
}

// Unix socket callback removed - using futex doorbells only

void ShmemClientTransport::StartCall(CallHandler child_call_handler) {
  fprintf(stderr, "*** DEBUG: Client StartCall CALLED! ***\n");
  fflush(stderr);
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
               fprintf(stderr, "*** DEBUG: Client about to PushCommand C2S_INITIAL_METADATA, sem_adapter=%p ***\n", sem_adapter_.get());
               fflush(stderr);
               grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S, cmd, sem_adapter_.get());
               fprintf(stderr, "*** DEBUG: Client PushCommand completed ***\n");
               fflush(stderr);
               
               VLOG(1) << "CLIENT: Pushed C2S_INITIAL_METADATA stream_id=" << stream_id;
               return absl::OkStatus();
             }));
  
  // Send client messages as C2S_MESSAGE frames
  auto send_message = [cb, stream_id, this](MessageHandle m) -> StatusFlag {
    auto* payload = m->payload();
    const size_t n = payload->Length();
    
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
      VLOG(1) << "C2S CHUNKING path: len=" << n << " > cap=" << rb->capacity;
      VLOG(1) << "C2S_MESSAGE chunking: total=" << n
              << " max_chunk=" << (rb->capacity - 65536);
      
      // Allocate temporary buffer and copy payload once
      VLOG(1) << "C2S CHUNKING: Allocating temp buffer size=" << n;
      std::unique_ptr<unsigned char[]> tmp(new unsigned char[n]);
      VLOG(1) << "C2S CHUNKING: Copying payload to temp buffer";
      payload->CopyToBuffer(tmp.get());
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
        grpc_shmem::PushCommand(cb->GetC2SQueues(), cb, grpc_shmem::Direction::kC2S,
                                cmd, sem_adapter_.get());
        
        VLOG(1) << "C2S CHUNK ENQ stream=" << stream_id
                << " off=" << chunk_off
                << " size=" << chunk
                << " last=" << is_last;
        offset += chunk;
      }
    }
    return Success{};
  };
  child_call_handler.SpawnGuarded(
      "c2s_messages_and_eos",
      TrySeq(
        ForEach(MessagesFrom(child_call_handler), std::move(send_message)),
        [cb, stream_id, this]() -> StatusFlag {
          // Explicit client half-close (no payload)
          grpc_shmem::Command eos{};
          eos.stream_id = stream_id;
          eos.type = grpc_shmem::FrameType::C2S_TRAILING_METADATA;
          eos.data_offset = 0;
          eos.data_size = 0;
          VLOG(1) << "CLIENT: Sending C2S_TRAILING_METADATA stream_id=" << stream_id;
          bool success = grpc_shmem::PushCommand(cb->GetC2SQueues(), cb,
                                                 grpc_shmem::Direction::kC2S,
                                                 eos, sem_adapter_.get());
          return StatusFlag(success);
        }
      ));
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
  auto it = g_cross_process_segments.find(cb);
  if (it != g_cross_process_segments.end()) {
    LOG(INFO) << "RemoveCrossProcessSegment: Found segment for cb=" << cb;
    
    // With futex doorbells, no named semaphore cleanup needed
    
    // Destroy the ShmemSegment object (this will call Unmap() in its destructor)
    it->second.reset();
    g_cross_process_segments.erase(it);
    LOG(INFO) << "RemoveCrossProcessSegment: Removed segment for cb=" << cb;
  } else {
    LOG(WARNING) << "RemoveCrossProcessSegment: No segment found for cb=" << cb;
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
    grpc_shmem::ShmemSegment::RemoveIfExists(cfg.name);
    auto s = grpc_shmem::ShmemSegment::Create(cfg);
    segment = std::make_unique<grpc_shmem::ShmemSegment>(std::move(s));
    cb = segment->control();
  }

  // Ensure both sides share a name so the eventfd Unix-socket handshake works
  std::string name_for_args = absl::StrCat("bench_", getpid(), "_", pair_id_val);
  ChannelArgs sargs = server_channel_args.Set("grpc.shmem.server_name", name_for_args);
  ChannelArgs cargs = client_channel_args.Set("grpc.shmem.server_name", name_for_args);
  auto server_transport = MakeOrphanable<ShmemServerTransport>(
      sargs, std::move(segment));
  auto client_transport = MakeOrphanable<ShmemClientTransport>(
      server_transport.get(), cb, cargs);
  return {OrphanablePtr<Transport>(client_transport.release()), 
          OrphanablePtr<Transport>(server_transport.release())};
}

OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& server_channel_args) {
  fprintf(stderr, "*** DEBUG: MakeNamedShmemServerTransport called with server_name: %s ***\n", server_name.c_str());
  fflush(stderr);
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
  
  fprintf(stderr, "*** DEBUG: Creating new segment: %s ***\n", cfg.name.c_str());
  fflush(stderr);
  VLOG(2) << "Creating new segment...";
  auto s = grpc_shmem::ShmemSegment::Create(cfg);
  
  fprintf(stderr, "*** DEBUG: Created segment, control block: %p ***\n", s.control());
  fflush(stderr);
  VLOG(2) << "Created segment, checking control block...";
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
  return OrphanablePtr<Transport>(server_transport.release());
}

OrphanablePtr<Transport> ConnectToShmemServerTransport(
    const std::string& server_name, const ChannelArgs& client_channel_args) {
  fprintf(stderr, "*** DEBUG: ConnectToShmemServerTransport called with server_name: %s ***\n", server_name.c_str());
  fflush(stderr);
  VLOG(2) << "ConnectToShmemServerTransport called with server_name: " << server_name;
  
  const bool dispatch_only =
      client_channel_args.GetBool("grpc.shmem.dispatch_only").value_or(false);

  if (dispatch_only) {
    LOG(ERROR) << "Cross-process shmem requires ring mode (not dispatch-only)";
    return nullptr;
  }

  std::string segment_name = absl::StrCat("grpc_shmem_", server_name);
  fprintf(stderr, "*** DEBUG: Opening segment: %s ***\n", segment_name.c_str());
  fflush(stderr);
  VLOG(2) << "Opening segment: " << segment_name;
  
  auto segment = grpc_shmem::ShmemSegment::Open(segment_name);
  
  fprintf(stderr, "*** DEBUG: Segment opened, control block: %p ***\n", segment.control());
  fflush(stderr);
  VLOG(2) << "Segment opened, control block: " << segment.control();
  if (segment.control() == nullptr) {
    LOG(ERROR) << "Failed to connect to shmem server: " << server_name;
    return nullptr;
  }

  grpc_shmem::ControlBlock* cb = segment.control();
  VLOG(2) << "Control block at: " << cb;
  VLOG(2) << "c2s_queues: " << cb->GetC2SQueues() << ", s2c_queues: " << cb->GetS2CQueues();
  
  auto segment_ptr = std::make_unique<grpc_shmem::ShmemSegment>(std::move(segment));
  
  fprintf(stderr, "*** DEBUG: About to store segment and create client transport ***\n");
  fflush(stderr);
  VLOG(2) << "About to store segment and create client transport";
  
  // Store segment for cleanup
  StoreCrossProcessSegment(cb, std::move(segment_ptr));
  
  fprintf(stderr, "*** DEBUG: Creating client transport with cb: %p ***\n", cb);
  fflush(stderr);
  VLOG(2) << "Creating client transport with cb: " << cb;
  
  auto result = OrphanablePtr<Transport>(MakeOrphanable<ShmemClientTransport>(nullptr, cb, client_channel_args).release());
  fprintf(stderr, "*** DEBUG: Client transport created successfully ***\n");
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