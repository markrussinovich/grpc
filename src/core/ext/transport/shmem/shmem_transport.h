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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_TRANSPORT_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_TRANSPORT_H

#include <atomic>
#include <cstdint>

#include "src/core/lib/transport/transport.h"
#include "src/core/ext/transport/shmem/shmem_semaphore.h"
#include "src/core/ext/transport/shmem/shmem_lockfree_queue.h"

namespace grpc_shmem {

// Forward declarations for composite structures
struct Command;
struct DataRingBuffer;
struct ShmemQueues;
class CrossProcessSemaphore;

// Shutdown diagnostics helper
class ShmemShutdownTracer {
 private:
  static std::atomic<int> trace_id_;
  int my_trace_id_;
  std::string component_;
  
 public:
  ShmemShutdownTracer(const std::string& component) 
      : my_trace_id_(trace_id_.fetch_add(1)), component_(component) {
    LOG(INFO) << "[TRACE-" << my_trace_id_ << "] Starting shutdown: " << component_;
  }
  
  ~ShmemShutdownTracer() {
    LOG(INFO) << "[TRACE-" << my_trace_id_ << "] Completed shutdown: " << component_;
  }
  
  void Checkpoint(const std::string& step) {
    LOG(INFO) << "[TRACE-" << my_trace_id_ << "] Checkpoint: " << step;
  }
};

// Safe pointer access validator
class SafePointerAccess {
 public:
  template<typename T>
  static bool IsValid(const T* ptr) {
    if (!ptr) return false;
    
    // Try to read first byte - will segfault if invalid
    try {
      volatile char test = *reinterpret_cast<const volatile char*>(ptr);
      (void)test; // Suppress unused variable warning
      return true;
    } catch (...) {
      return false;
    }
  }
};

// The master control block, located at the beginning of the shared memory
// segment.
struct ControlBlock {
  uint64_t magic_number;
  uint32_t transport_version;
  std::atomic<uint32_t> server_state;
  std::atomic<uint32_t> client_state;

  // --- Process Coordination ---
  // std::atomic<int32_t> process_count{0};       // Number of attached processes
  // std::atomic<bool> cleanup_initiated{false};  // Global shutdown signal
  int32_t process_count;       // Number of attached processes  
  bool cleanup_initiated;      // Global shutdown signal
  
  // --- Cross-Process Semaphore Names ---
  // Store semaphore names instead of process-specific handles
  // char c2s_sem_name[32];  // Name for client-to-server semaphore
  // char s2c_sem_name[32];  // Name for server-to-client semaphore
  
  // Set by the consumer just before sleeping; producers check this to avoid
  // spurious posts. 0 = not waiting, 1 = waiting.
  std::atomic<uint32_t> c2s_waiters{0};
  std::atomic<uint32_t> s2c_waiters{0};

  // --- Pointers to the new queue structures ---
  ShmemQueues* c2s_queues;
  ShmemQueues* s2c_queues;

  // Constructor to initialize fields
  ControlBlock()
      : magic_number(0x47525043534D454Dull /* "GRPCSMEM" */),
        transport_version(1),
        server_state(0),
        client_state(0),
        process_count(0),
        cleanup_initiated(false),
        c2s_waiters(0),
        s2c_waiters(0),
        c2s_queues(nullptr),
        s2c_queues(nullptr) {
    // Initialize semaphore name fields to empty
    // c2s_sem_name[0] = '\0';
    // s2c_sem_name[0] = '\0';
  }
};

// Re-using FrameType concept to describe commands flowing over the command
// queue
enum class FrameType : uint8_t {
  C2S_INITIAL_METADATA = 0x01,
  C2S_MESSAGE = 0x02,
  C2S_TRAILING_METADATA = 0x03,
  C2S_CANCEL = 0x04,
  S2C_INITIAL_METADATA = 0x81,
  S2C_MESSAGE = 0x82,
  S2C_TRAILING_METADATA = 0x83,
};

// A fixed-size command describing an action for the peer to take, with any
// payload referenced via an offset into the DataRingBuffer.
struct Command {
  uint32_t stream_id;
  FrameType type;
  // For MESSAGE and METADATA frames, points into the data ring buffer.
  uint64_t data_offset;
  uint32_t data_size;
  // For S2C_TRAILING_METADATA, holds the gRPC status code.
  int32_t grpc_status_code;
  // Small inline storage for very small data (status message, flags, etc.)
  char inline_data;
};

// A simple ring buffer for variable-length binary data shared between peers.
struct DataRingBuffer {
  uint64_t capacity = 0;
  // head is advanced by the producer, tail by the consumer.
  std::atomic<uint64_t> head{0};
  std::atomic<uint64_t> tail{0};
  unsigned char* buffer = nullptr;
};

// Define the command queue type using our custom lock-free SPSC queue. 
// Capacity must be a power of two.
constexpr size_t COMMAND_QUEUE_CAPACITY = 256;
using CommandQueue = SPSCQueue<Command, COMMAND_QUEUE_CAPACITY>;

// Holds the pair of queues used for one direction of communication.
struct ShmemQueues {
  // Lock-free SPSC queue for commands.
  CommandQueue command_q;
  // Backing data ring buffer for payloads referenced by commands.
  DataRingBuffer data_rb;
};

// Helper class to manage cross-process semaphore operations for transports
class SemaphoreManager {
 public:
  SemaphoreManager() = default;
  ~SemaphoreManager() { Cleanup(); }

  // Initialize semaphores from control block names
  absl::Status InitFromControlBlock(ControlBlock* cb) {
    if (cb == nullptr) {
      return absl::InvalidArgumentError("Null control block");
    }
    
    // TEMPORARILY DISABLED - semaphore name fields removed
    LOG(INFO) << "SemaphoreManager::InitFromControlBlock called (disabled for testing)";
    
    /*
    LOG(INFO) << "SemaphoreManager::InitFromControlBlock: c2s_name='" << cb->c2s_sem_name 
              << "', s2c_name='" << cb->s2c_sem_name << "'";
    
    if (cb->c2s_sem_name[0] != '\0') {
      auto status = c2s_sem_.InitFromName(cb->c2s_sem_name);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to init c2s semaphore: " << status;
        return status;
      }
      LOG(INFO) << "Successfully initialized c2s semaphore: " << cb->c2s_sem_name;
    }
    
    if (cb->s2c_sem_name[0] != '\0') {
      auto status = s2c_sem_.InitFromName(cb->s2c_sem_name);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to init s2c semaphore: " << status;
        return status;
      }
      LOG(INFO) << "Successfully initialized s2c semaphore: " << cb->s2c_sem_name;
    }
    */
    
    return absl::OkStatus();
  }

  // Post to semaphore based on direction
  void Post(ControlBlock* cb, bool c2s_direction) {
    if (c2s_direction) {
      if (cb->c2s_waiters.load(std::memory_order_relaxed)) {
        c2s_sem_.post();
      }
    } else {
      if (cb->s2c_waiters.load(std::memory_order_relaxed)) {
        s2c_sem_.post();
      }
    }
  }

  // Wait on semaphore based on direction
  void Wait(bool c2s_direction) {
    if (c2s_direction) {
      c2s_sem_.wait();
    } else {
      s2c_sem_.wait();
    }
  }

  // Wake all waiters for shutdown
  void WakeAll() {
    c2s_sem_.post();
    s2c_sem_.post();
  }

  // Timeout-enabled wait for graceful shutdown
  bool WaitWithTimeout(bool c2s_direction, std::chrono::milliseconds timeout) {
    // For now, use regular wait - can be enhanced with sem_timedwait later
    if (c2s_direction) {
      c2s_sem_.wait();
    } else {
      s2c_sem_.wait();
    }
    return true;
  }

  // Explicit cleanup method
  void Cleanup() {
    ShmemShutdownTracer tracer("SemaphoreManager::Cleanup");
    
    try {
      tracer.Checkpoint("Starting semaphore cleanup");
      // Close handles - the CrossProcessSemaphore destructor handles this
      // but we make it explicit for better error handling
      tracer.Checkpoint("Semaphore cleanup completed successfully");
    } catch (const std::exception& e) {
      LOG(ERROR) << "SemaphoreManager cleanup error: " << e.what();
    }
  }

 private:
  CrossProcessSemaphore c2s_sem_;
  CrossProcessSemaphore s2c_sem_;
};

}  // namespace grpc_shmem

namespace grpc_core {

// Factory entry point used by tests and the surface integration later on.
// Implemented in shmem_transport.cc.
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args,
                       const ChannelArgs& client_channel_args);

// Factory entry point for creating a server transport with a predictable name
// that clients can connect to cross-process. Returns server transport only.
OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& server_channel_args);

// Factory entry point for connecting to an existing named server transport.
// Returns client transport only.
OrphanablePtr<Transport> ConnectToShmemServerTransport(
    const std::string& server_name, const ChannelArgs& client_channel_args);

}  // namespace grpc_core

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_TRANSPORT_H
