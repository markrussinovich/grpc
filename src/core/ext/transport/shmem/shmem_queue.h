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

// Queue/data-path helpers for shmem transport (producer/consumer logic).
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

enum class Direction { kC2S, kS2C };

// REVERTED: Remove SemaphoreManager forward declaration for baseline testing

// Compute free space in the ring (in bytes), using monotonic head/tail.
inline uint64_t RingFreeBytes(const DataRingBuffer& rb) {
  const uint64_t head = rb.head.load(std::memory_order_acquire);
  const uint64_t tail = rb.tail.load(std::memory_order_acquire);
  const uint64_t used = head - tail;  // monotonic counters
  if (used > rb.capacity) return 0;   // saturated -> treat as full
  return rb.capacity - used;
}

// Reserve a contiguous region of 'size' bytes in the ring buffer.
// Returns true and fills out_offset (0..capacity-1) on success. Blocks by
// spinning until space is available. This function is SPSC-safe.
bool ReserveContiguous(DataRingBuffer* rb, uint32_t size, uint64_t* out_offset);

// Reserve space allowing wrapping - for very large messages
// Returns true and fills out_offset. May wrap around ring buffer.
bool ReserveWrapping(DataRingBuffer* rb, uint32_t size, uint64_t* out_offset);

// Reserves space for a contiguous write, possibly wrapping. Returns:
//   - out_offset: where to place the payload (mod capacity)
//   - out_pad: bytes of padding to skip to reach offset 0 (0 if no pad needed)
// The function only moves rb->head. It NEVER touches rb->tail.
bool ReserveForWrite(DataRingBuffer* rb, uint32_t size,
                     uint64_t* out_offset, uint32_t* out_pad);

// Blocking version of ReserveForWrite that waits with backoff until space is available.
// This prevents dropping required frames when the ring is momentarily full after wraps.
inline bool ReserveForWriteBlocking(DataRingBuffer* rb,
                                    uint32_t size,
                                    uint64_t* out_offset,
                                    uint32_t* out_pad) {
  // Spin/yield with a short sleep so the peer can process PAD/frames and advance tail.
  for (int attempts = 0; ; ++attempts) {
    if (ReserveForWrite(rb, size, out_offset, out_pad)) return true;
    if (attempts < 50) {
      std::this_thread::yield();
    } else if (attempts < 200) {
      // exponential-ish backoff
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }
}

// Release 'size' bytes previously consumed starting from some offset; simply
// advance tail (consumer side responsibility).
inline void Release(DataRingBuffer* rb, uint32_t size) {
  // Add protection against tail advancing beyond head
  uint64_t current_head = rb->head.load(std::memory_order_acquire);
  uint64_t current_tail = rb->tail.load(std::memory_order_relaxed);
  uint64_t new_tail = current_tail + size;
  
  // Prevent tail from advancing beyond head which would corrupt the queue
  if (new_tail <= current_head) {
    rb->tail.store(new_tail, std::memory_order_release);
  }
}

// Forward declaration for semaphore adapter
class TransportSemaphoreAdapter {
 public:
  virtual ~TransportSemaphoreAdapter() = default;
  virtual void Post(ControlBlock* cb, bool is_c2s) = 0;
  virtual void Wait(bool is_c2s) = 0;
  // Batching support - returns true if should post after adding this command
  virtual bool ShouldPost(bool is_c2s, size_t bytes_added, int frames_added) { return true; }
  
  // OPTIMIZATION: Return file descriptor for event loop integration
  // Returns -1 if FD-based polling not supported
  virtual int fd(bool is_c2s) { return -1; }
};

// Push a command and optionally wake the sleeping peer if the queue was empty
// before the push. Returns true if the command was pushed.
bool PushCommand(ShmemQueues* q, ControlBlock* cb, Direction dir,
                 const Command& cmd, TransportSemaphoreAdapter* sem_adapter = nullptr);

// Pop a command using hybrid spin-then-wait. Spins for spin_iters attempts;
// upon empty, waits on the appropriate semaphore. Returns true with a command
// when successful.
bool PopCommandHybrid(ShmemQueues* q, ControlBlock* cb, Direction dir,
                      int spin_iters, Command* out, TransportSemaphoreAdapter* sem_adapter = nullptr);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
