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

  // --- Lightweight Synchronization Semaphores ---
  // Used to wake a sleeping reader thread when the command queue transitions
  // from empty to non-empty.
  // Cross-process compatible: Both semaphores managed by name outside shared memory
  char c2s_sem_placeholder[64];  // Reserved space (semaphores managed by name)
  char s2c_sem_placeholder[64];  // Reserved space (semaphores managed by name)
  // Set by the consumer just before sleeping; producers check this to avoid
  // spurious posts. 0 = not waiting, 1 = waiting.
  std::atomic<uint32_t> c2s_waiters{0};
  std::atomic<uint32_t> s2c_waiters{0};

  // --- Process coordination for cross-process cleanup ---
  std::atomic<int32_t> process_count{0};  // Number of attached processes

  // --- Cross-process semaphore names (stored in shared memory) ---
  char c2s_sem_name[64];  // Client-to-server semaphore name
  char s2c_sem_name[64];  // Server-to-client semaphore name

  // --- Offsets to the queue structures (relative to segment base) ---  
  uint64_t c2s_queues_offset;  // Offset from segment base to c2s queues
  uint64_t s2c_queues_offset;  // Offset from segment base to s2c queues

  // Constructor to initialize fields and semaphores
  ControlBlock()
      : magic_number(0x47525043534D454Dull /* "GRPCSMEM" */),
        transport_version(1),
        server_state(0),
        client_state(0),
        c2s_waiters(0),
        s2c_waiters(0),
        process_count(0),
        c2s_queues_offset(0),
        s2c_queues_offset(0) {
    // Initialize semaphore name arrays to empty
    c2s_sem_name[0] = '\0';
    s2c_sem_name[0] = '\0';
  }
  
  // Helper methods to get actual queue pointers from offsets (cross-process safe)
  ShmemQueues* GetC2SQueues() {
    if (c2s_queues_offset == 0) return nullptr;
    char* base = reinterpret_cast<char*>(this);
    return reinterpret_cast<ShmemQueues*>(base + c2s_queues_offset);
  }
  
  ShmemQueues* GetS2CQueues() {
    if (s2c_queues_offset == 0) return nullptr;
    char* base = reinterpret_cast<char*>(this);
    return reinterpret_cast<ShmemQueues*>(base + s2c_queues_offset);
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
  // Store buffer as offset from segment base for cross-process compatibility
  uint64_t buffer_offset = 0;
  
  // Helper method to get actual buffer pointer (cross-process safe)
  unsigned char* GetBuffer(void* segment_base) {
    if (buffer_offset == 0) return nullptr;
    return reinterpret_cast<unsigned char*>(
        static_cast<char*>(segment_base) + buffer_offset);
  }
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

// Lightweight adapter to provide semaphore manager interface using transport's own semaphores
class TransportSemaphoreAdapter {
 public:
  TransportSemaphoreAdapter(CrossProcessSemaphore* c2s_sem, CrossProcessSemaphore* s2c_sem)
      : c2s_sem_(c2s_sem), s2c_sem_(s2c_sem) {}
  
  // Post to semaphore based on direction
  void Post(ControlBlock* cb, bool c2s_direction) {
    std::atomic<uint32_t>* waiters = c2s_direction ? &cb->c2s_waiters : &cb->s2c_waiters;
    if (waiters->load(std::memory_order_relaxed)) {
      CrossProcessSemaphore* sem = c2s_direction ? c2s_sem_ : s2c_sem_;
      if (sem) {
        sem->post();
      }
    }
  }

  // Wait on semaphore based on direction
  void Wait(bool c2s_direction) {
    CrossProcessSemaphore* sem = c2s_direction ? c2s_sem_ : s2c_sem_;
    if (sem) {
      sem->wait();
    }
  }

 private:
  CrossProcessSemaphore* c2s_sem_;
  CrossProcessSemaphore* s2c_sem_;
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
