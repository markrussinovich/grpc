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
  
  // For medium/large messages (>128KB), use a more aggressive strategy
  // 256KB+ messages need more sophisticated handling due to fragmentation
  const bool medium_message = size > (128 * 1024);
  const bool large_message = size > (1024 * 1024);
  const int max_attempts = large_message ? 100 : (medium_message ? 50 : 10);
  
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
      } else if (medium_message) {
        // For medium/large messages, aggressively advance tail to create space
        // Use more conservative advancement for medium messages
        const uint64_t advance_amount = large_message ? 
          std::min(static_cast<uint64_t>(size / 2), rb->capacity / 8) :
          std::min(static_cast<uint64_t>(size / 4), rb->capacity / 16);
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
      uint64_t expected = head;
      if (rb->head.compare_exchange_weak(expected, new_head,
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
        uint64_t expected = head;
        if (rb->head.compare_exchange_weak(expected, new_head,
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
      uint64_t expected_tail = current_tail;
      if (rb->tail.compare_exchange_weak(expected_tail, new_tail,
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
      uint64_t expected = head;
      if (rb->head.compare_exchange_weak(expected, new_head,
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

static inline void Post(ControlBlock* cb, Direction dir, grpc_shmem::TransportSemaphoreAdapter* sem_adapter) {
  const char* dir_name = (dir == Direction::kC2S) ? "C2S" : "S2C";
  VLOG(3) << "Post " << dir_name << " - sem_adapter: " << sem_adapter;
  if (sem_adapter) {
    VLOG(3) << "Post " << dir_name << " - calling sem_adapter->Post";
    // Use cross-process semaphores via semaphore adapter
    sem_adapter->Post(cb, dir == Direction::kC2S);
    VLOG(3) << "Post " << dir_name << " - sem_adapter->Post completed";
  } else {
    // Fallback: Skip posting if no semaphore adapter available
    LOG(WARNING) << "Post " << dir_name << " - no semaphore adapter available";
  }
}

static inline void Wait(ControlBlock* cb, Direction dir, grpc_shmem::TransportSemaphoreAdapter* sem_adapter) {
  const char* dir_name = (dir == Direction::kC2S) ? "C2S" : "S2C";
  VLOG(3) << "Wait " << dir_name << " - sem_adapter: " << sem_adapter;
  if (sem_adapter) {
    VLOG(3) << "Wait " << dir_name << " - calling sem_adapter->Wait";
    // Use cross-process semaphores via semaphore adapter
    sem_adapter->Wait(dir == Direction::kC2S);
    VLOG(3) << "Wait " << dir_name << " - sem_adapter->Wait returned (woke up!)";
  } else {
    // Fallback: Skip waiting if no semaphore adapter available
    LOG(WARNING) << "Wait " << dir_name << " - no semaphore adapter available";
  }
}

bool PushCommand(ShmemQueues* q, ControlBlock* cb, Direction dir,
                 const Command& cmd, grpc_shmem::TransportSemaphoreAdapter* sem_adapter) {
  // Check if queue was empty before pushing
  const bool was_empty = q->command_q.empty();
  const bool ok = q->command_q.push(cmd);
  if (!ok) return false;

  // Publish the command before checking conditions
  std::atomic_thread_fence(std::memory_order_release);

  // Get waiter flag for this direction
  std::atomic<uint32_t>* waiters =
      (dir == Direction::kC2S) ? &cb->c2s_waiters : &cb->s2c_waiters;

  // Post if: (1) peer declared it's sleeping, OR (2) queue was empty (to handle startup)
  // This hybrid approach should eliminate both race conditions and startup issues
  if (waiters->load(std::memory_order_acquire) != 0 || was_empty) {
    Post(cb, dir, sem_adapter);
  }

  return true;
}

bool PopCommandHybrid(ShmemQueues* q, ControlBlock* cb, Direction dir,
                      int spin_iters, Command* out, grpc_shmem::TransportSemaphoreAdapter* sem_adapter) {
  const char* dir_name = (dir == Direction::kC2S) ? "C2S" : "S2C";
  static int pop_call_count = 0;
  pop_call_count++;
  
  if (pop_call_count <= 10 || pop_call_count % 50 == 0) {
    VLOG(3) << "PopCommandHybrid " << dir_name << " - call #" << pop_call_count;
  }
  
  Command tmp;
  
  // Adaptive spinning: more spins for high throughput scenarios  
  const int effective_spins = std::max(spin_iters, 8);
  for (int i = 0; i < effective_spins; ++i) {
    if (q->command_q.pop(tmp)) {
      *out = tmp;
      if (pop_call_count <= 10) {
        VLOG(3) << "PopCommandHybrid " << dir_name << " - found command in spin loop";
      }
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
  if (pop_call_count <= 10) {
    VLOG(3) << "PopCommandHybrid " << dir_name << " - setting waiter flag";
  }
  waiters->store(1, std::memory_order_release);
  
  // Memory barrier to ensure waiter flag is visible before checking queue
  std::atomic_thread_fence(std::memory_order_seq_cst);
  
  // One last check after setting waiters flag and memory barrier
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    waiters->store(0, std::memory_order_relaxed);
    return true;
  }
  
  // Sleep until woken up by producer (event-driven)
  // RACE CONDITION FIX: Always use semaphores for proper cross-process coordination
  // The startup delay logic was causing race conditions
  Wait(cb, dir, sem_adapter);
  waiters->store(0, std::memory_order_relaxed);
  
  // Upon wake, try again (one attempt)
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    return true;
  }
  return false;
}

}  // namespace grpc_shmem
