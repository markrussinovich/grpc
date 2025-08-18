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

#include "src/core/ext/transport/shmem/shmem_queue.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace grpc_shmem {

bool ReserveContiguous(DataRingBuffer* rb, uint32_t size,
                       uint64_t* out_offset) {
  if (size > rb->capacity) return false;
  
  // For large messages (>1MB), use a more aggressive strategy
  const bool large_message = size > (1024 * 1024);
  const int max_attempts = large_message ? 100 : 10;
  
  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    const uint64_t head = rb->head.load(std::memory_order_relaxed);
    const uint64_t tail = rb->tail.load(std::memory_order_acquire);
    const uint64_t used = head - tail;
    
    // Ensure we have enough total space
    if (used + size > rb->capacity) {
      if (attempt < 5) {
        std::this_thread::yield();
      } else if (attempt < 20) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
      } else if (large_message) {
        // For large messages, aggressively advance tail to create space
        const uint64_t advance_amount = std::min(static_cast<uint64_t>(size / 2), 
                                                rb->capacity / 8);
        rb->tail.fetch_add(advance_amount, std::memory_order_acq_rel);
      } else {
        return false;
      }
      continue;
    }
    
    const uint64_t offset = head % rb->capacity;
    
    // Strategy 1: Try normal contiguous allocation first
    if (offset + size <= rb->capacity) {
      const uint64_t new_head = head + size;
      if (rb->head.compare_exchange_weak(const_cast<uint64_t&>(head), new_head,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
        *out_offset = offset;
        return true;
      }
      continue; // CAS failed, retry
    }
    
    // Strategy 2: Use space at beginning if available
    const uint64_t tail_offset = tail % rb->capacity;
    const uint64_t space_at_start = (tail_offset == 0) ? rb->capacity : tail_offset;
    
    if (size <= space_at_start) {
      // Check if we can safely wrap to the beginning
      const uint64_t required_tail_advancement = rb->capacity - offset;
      const uint64_t new_head = head + required_tail_advancement + size;
      
      if (new_head - tail <= rb->capacity) {
        if (rb->head.compare_exchange_weak(const_cast<uint64_t&>(head), new_head,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
          *out_offset = 0;
          return true;
        }
        continue;
      }
    }
    
    // Strategy 3: For very large messages, force compaction
    if (large_message && attempt > 50) {
      // Reset ring to minimize fragmentation
      const uint64_t current_tail = rb->tail.load(std::memory_order_acquire);
      const uint64_t new_tail = current_tail + (used / 2);
      if (rb->tail.compare_exchange_weak(const_cast<uint64_t&>(current_tail), new_tail,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
        // Force a fresh attempt after compaction
        attempt = 0;
      }
    }
    
    // Progressive backoff
    if (attempt < 5) {
      // Fast path: just yield
    } else if (attempt < 20) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::nanoseconds(attempt * 10));
    }
  }
  
  return false;
}

bool ReserveWrapping(DataRingBuffer* rb, uint32_t size, uint64_t* out_offset) {
  if (size > rb->capacity) return false;
  
  // For wrapping reserve, we just need enough total space, don't care about contiguity
  for (int spin = 0; spin < 50; ++spin) {  // More attempts for wrapping
    const uint64_t head = rb->head.load(std::memory_order_relaxed);
    const uint64_t tail = rb->tail.load(std::memory_order_acquire);
    const uint64_t used = head - tail;
    
    if (used + size <= rb->capacity) {
      // We have enough space, reserve it (may wrap)
      const uint64_t new_head = head + size;
      if (rb->head.compare_exchange_weak(const_cast<uint64_t&>(head), new_head,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
        *out_offset = head % rb->capacity;
        return true;
      }
    }
    
    // Wait for space
    if (spin < 10) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }
  return false;
}

static inline void Post(ControlBlock* cb, Direction dir) {
  if (dir == Direction::kC2S) {
    // Use relaxed ordering since eventfd provides synchronization
    if (cb->c2s_waiters.load(std::memory_order_relaxed)) cb->c2s_sem.post();
  } else {
    if (cb->s2c_waiters.load(std::memory_order_relaxed)) cb->s2c_sem.post();
  }
}

static inline void Wait(ControlBlock* cb, Direction dir) {
  if (dir == Direction::kC2S)
    cb->c2s_sem.wait();
  else
    cb->s2c_sem.wait();
}

bool PushCommand(ShmemQueues* q, ControlBlock* cb, Direction dir,
                 const Command& cmd) {
  // Check if queue was empty before pushing - if so, we need to signal
  const bool was_empty = q->command_q.empty();
  const bool ok = q->command_q.push(cmd);
  if (ok && was_empty) {
    // Only post if queue was empty AND consumer is waiting
    // Further reduces kernel transitions by checking waiter state
    std::atomic<uint32_t>* waiters = (dir == Direction::kC2S) ? &cb->c2s_waiters : &cb->s2c_waiters;
    if (waiters->load(std::memory_order_relaxed)) {
      Post(cb, dir);
    }
  }
  return ok;
}

bool PopCommandHybrid(ShmemQueues* q, ControlBlock* cb, Direction dir,
                      int spin_iters, Command* out) {
  Command tmp;
  
  // Adaptive spinning: more spins for high throughput scenarios  
  const int effective_spins = std::max(spin_iters, 8);
  for (int i = 0; i < effective_spins; ++i) {
    if (q->command_q.pop(tmp)) {
      *out = tmp;
      return true;
    }
    // Yield every few iterations to avoid excessive CPU usage
    if (i > 0 && i % 4 == 0) {
      std::this_thread::yield();
    }
  }
  
  // Declare intent to sleep, then re-check before actually sleeping to avoid
  // lost wakeups
  std::atomic<uint32_t>* waiters = (dir == Direction::kC2S) ? &cb->c2s_waiters : &cb->s2c_waiters;
  waiters->store(1, std::memory_order_release);
  
  // One last check after setting waiters flag
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    waiters->store(0, std::memory_order_relaxed);
    return true;
  }
  
  // Sleep until woken up by producer
  Wait(cb, dir);
  waiters->store(0, std::memory_order_relaxed);
  
  // Upon wake, try again (one attempt)
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    return true;
  }
  return false;
}

}  // namespace grpc_shmem
