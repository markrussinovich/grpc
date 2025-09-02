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

#include "absl/log/log.h"

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
    const uint64_t used = grpc_shmem::RingUsedBytes(head, tail, rb->capacity);
    
    // Ensure we have enough total space
    if (used + size > rb->capacity) {
      if (attempt < 5) {
        std::this_thread::yield();
      } else if (attempt < 20) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
      } else {
        return false;
      }
      continue;
    }
    
    const uint64_t offset = head % rb->capacity;
    
    // Strategy 1: Try normal contiguous allocation first
    if (offset + size <= rb->capacity) {
      const uint64_t new_head = grpc_shmem::AdvanceHeadContiguous(head, size);
      if (grpc_shmem::TryAdvanceHead(rb, head, new_head)) {
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
      const uint64_t new_head = grpc_shmem::AdvanceHeadWithPadding(head, size, required_tail_advancement);
      
      if (new_head - tail <= rb->capacity) {
        if (grpc_shmem::TryAdvanceHead(rb, head, new_head)) {
          *out_offset = 0;
          return true;
        }
        continue;
      }
    }
    
    // Strategy 3: For very large messages was removed - writer no longer touches tail
    
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
    const uint64_t used = grpc_shmem::RingUsedBytes(head, tail, rb->capacity);
    
    if (used + size <= rb->capacity) {
      // We have enough space, reserve it (may wrap)
      const uint64_t new_head = grpc_shmem::AdvanceHeadContiguous(head, size);
      if (grpc_shmem::TryAdvanceHead(rb, head, new_head)) {
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
  // Fast path: try a few times without sleeping
  for (int attempts = 0; ; ++attempts) {
    const bool was_empty = q->command_q.empty();   // approximate is fine
    if (q->command_q.push(cmd)) {
      std::atomic_thread_fence(std::memory_order_release);

      // Log successful enqueue for important frame types
      switch (cmd.type) {
        case grpc_shmem::FrameType::DATA_PAD:
        case grpc_shmem::FrameType::C2S_MESSAGE:
        case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK:
        case grpc_shmem::FrameType::C2S_MESSAGE_CHUNK_LAST:
        case grpc_shmem::FrameType::C2S_TRAILING_METADATA:
        case grpc_shmem::FrameType::S2C_INITIAL_METADATA:
        case grpc_shmem::FrameType::S2C_MESSAGE:
        case grpc_shmem::FrameType::S2C_MESSAGE_CHUNK:
        case grpc_shmem::FrameType::S2C_MESSAGE_CHUNK_LAST:
        case grpc_shmem::FrameType::S2C_TRAILING_METADATA:
          VLOG(1) << "PushCommand OK dir=" << (dir == Direction::kC2S ? "C2S" : "S2C")
                  << " type=" << static_cast<int>(cmd.type)
                  << " stream=" << cmd.stream_id
                  << " size=" << cmd.data_size;
          break;
        default: break;
      }

      // Use batching logic if adapter supports it, otherwise fall back to old logic
      bool should_post = true;
      if (sem_adapter) {
        should_post = sem_adapter->ShouldPost(dir == Direction::kC2S, 
                                             sizeof(Command) + cmd.data_size, 1, was_empty);
      } else {
        // Legacy fallback: wake based on waiter flag or first-item heuristic
        should_post = was_empty;
      }
      
      if (should_post) {
        VLOG(2) << "PushCommand: Posting to " << (dir == Direction::kC2S ? "C2S" : "S2C") 
                << " after enqueue (was_empty=" << was_empty << ")";
        Post(cb, dir, sem_adapter);
      } else {
        VLOG(2) << "PushCommand: Skipping post to " << (dir == Direction::kC2S ? "C2S" : "S2C") 
                << " due to batching (was_empty=" << was_empty << ")";
      }
      return true;
    }

    // Queue is full — nudge the peer and backoff
    Post(cb, dir, sem_adapter);  // help wake the reader if it's asleep

    if (attempts < 32) {
      std::this_thread::yield();
    } else if (attempts < 128) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    } else {
      // Diagnostic: print once every ~1ms while wedged
      static thread_local int warned = 0;
      if ((++warned % 100) == 1) {
        VLOG(1) << "PushCommand backoff: dir=" << (dir == Direction::kC2S ? "C2S" : "S2C");
      }
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    }
  }
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
  if (pop_call_count <= 10) {
    VLOG(3) << "PopCommandHybrid " << dir_name << " - preparing to wait";
    VLOG(3) << "PopCommandHybrid: sleeping dir=" << dir_name
            << " cmdq_empty=" << q->command_q.empty();
  }
  
  // One last check before waiting
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    return true;
  }
  
  // Sleep until woken up by producer (event-driven)
  // Uses futex doorbell for efficient cross-process coordination
  Wait(cb, dir, sem_adapter);
  
  // Upon wake, try again (one attempt)
  if (q->command_q.pop(tmp)) {
    *out = tmp;
    return true;
  }
  return false;
}

bool ReserveForWrite(DataRingBuffer* rb, uint32_t size,
                     uint64_t* out_offset, uint32_t* out_pad) {
  if (size > rb->capacity) return false;

  for (;;) {
    uint64_t head = rb->head.load(std::memory_order_relaxed);
    uint64_t tail = rb->tail.load(std::memory_order_acquire);
    uint64_t used = grpc_shmem::RingUsedBytes(head, tail, rb->capacity);
    if (used + size > rb->capacity) {
      VLOG(1) << "ReserveForWrite FAIL size=" << size
              << " head=" << head << " tail=" << tail
              << " used=" << used
              << " cap=" << rb->capacity << " (not enough total space)";
      return false;  // not enough total space
    }

    uint64_t end_off = head % rb->capacity;
    uint64_t free_to_end = rb->capacity - end_off;

    uint64_t new_head;
    uint32_t pad = 0;
    uint64_t offset;

    if (size <= free_to_end) {
      // Fits to end; no pad needed.
      new_head = grpc_shmem::AdvanceHeadContiguous(head, size);
      offset = end_off;
      pad = 0;
    } else {
      // Needs wrap; ensure total free includes the pad.
      if (used + size + free_to_end > rb->capacity) {
        VLOG(1) << "ReserveForWrite FAIL size=" << size
                << " head=" << head << " tail=" << tail
                << " used=" << used
                << " end_off=" << end_off
                << " free_to_end=" << free_to_end
                << " cap=" << rb->capacity << " (not enough space including pad)";
        return false;  // not enough space including pad
      }
      new_head = grpc_shmem::AdvanceHeadWithPadding(head, size, free_to_end);
      offset = 0;                              // write starts at beginning
      pad = static_cast<uint32_t>(free_to_end);
    }

    if (grpc_shmem::TryAdvanceHead(rb, head, new_head)) {
      *out_offset = offset;   // mod capacity
      *out_pad = pad;         // 0 if no wrap
      return true;
    }
    // CAS failed — retry with updated head/tail
  }
}

}  // namespace grpc_shmem
