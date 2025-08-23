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
struct ControlBlock;

// The master control block, located at the beginning of the shared memory
// segment.
struct ControlBlock {
  // NEW: make magic/version atomics (safe publishing across processes)
  std::atomic<uint64_t> magic_number{0};
  std::atomic<uint32_t> transport_version{0};

  // Optional counters / state
  std::atomic<int32_t>  process_count{0};
  std::atomic<uint32_t> server_state{0};
  std::atomic<uint32_t> client_state{0};

  // --- Lightweight Synchronization Semaphores ---
  // Used to wake a sleeping reader thread when the command queue transitions
  // from empty to non-empty.
  EventFdSemaphore c2s_sem;
  EventFdSemaphore s2c_sem;
  // Set by the consumer just before sleeping; producers check this to avoid
  // spurious posts. 0 = not waiting, 1 = waiting.
  std::atomic<uint32_t> c2s_waiters{0};
  std::atomic<uint32_t> s2c_waiters{0};

  // NEW: offsets to the two ShmemQueues blocks, relative to this ControlBlock*
  uint64_t c2s_queues_offset = 0;
  uint64_t s2c_queues_offset = 0;

  // NEW: names for named semaphores (POSIX shm-safe)
  char c2s_sem_name[64] = {0};
  char s2c_sem_name[64] = {0};

  // Helpers (these are used throughout your transport)
  inline ShmemQueues* GetC2SQueues() {
    return reinterpret_cast<ShmemQueues*>(
        reinterpret_cast<char*>(this) + c2s_queues_offset);
  }
  inline ShmemQueues* GetS2CQueues() {
    return reinterpret_cast<ShmemQueues*>(
        reinterpret_cast<char*>(this) + s2c_queues_offset);
  }
  inline const ShmemQueues* GetC2SQueues() const {
    return const_cast<ControlBlock*>(this)->GetC2SQueues();
  }
  inline const ShmemQueues* GetS2CQueues() const {
    return const_cast<ControlBlock*>(this)->GetS2CQueues();
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
// Valid across processes by storing an offset, not a raw pointer.
struct DataRingBuffer {
  uint64_t capacity = 0;
  // head is advanced by the producer, tail by the consumer.
  std::atomic<uint64_t> head{0};
  std::atomic<uint64_t> tail{0};
  // NEW: offset from ControlBlock* base to the data buffer
  uint64_t buffer_offset = 0;

  inline unsigned char* GetBuffer(ControlBlock* cb) const {
    return reinterpret_cast<unsigned char*>(
        reinterpret_cast<char*>(cb) + buffer_offset);
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

}  // namespace grpc_shmem

namespace grpc_core {

// Factory entry point used by tests and the surface integration later on.
// Implemented in shmem_transport.cc.
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args);

// Two-argument overload for distinct server and client channel args
std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args,
                       const ChannelArgs& client_channel_args);

// Create a named server transport for cross-process communication
OrphanablePtr<Transport> MakeNamedShmemServerTransport(
    const std::string& server_name, const ChannelArgs& args);

// Connect to an existing named shmem server transport
OrphanablePtr<Transport> ConnectToShmemServerTransport(
    const std::string& server_name, const ChannelArgs& args);

}  // namespace grpc_core

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_TRANSPORT_H
