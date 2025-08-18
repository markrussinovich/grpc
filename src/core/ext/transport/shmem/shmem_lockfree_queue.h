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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LOCKFREE_QUEUE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LOCKFREE_QUEUE_H

#include <atomic>
#include <cstddef>

namespace grpc_shmem {

// Lock-free single-producer single-consumer (SPSC) queue.
// Replacement for boost::lockfree::spsc_queue with the same API.
// 
// This implementation uses a ring buffer with atomic head/tail pointers.
// Capacity must be a power of two for efficient modulo operations.
template <typename T, size_t Capacity>
class SPSCQueue {
  static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of two");
  static_assert(Capacity > 1, "Capacity must be greater than 1");
  
 public:
  SPSCQueue() : head_(0), tail_(0) {}
  
  // Producer: Push an element to the queue.
  // Returns true if successful, false if queue is full.
  bool push(const T& item) {
    const size_t current_tail = tail_.load(std::memory_order_relaxed);
    const size_t next_tail = (current_tail + 1) & (Capacity - 1);
    
    if (next_tail == head_.load(std::memory_order_acquire)) {
      // Queue is full
      return false;
    }
    
    buffer_[current_tail] = item;
    tail_.store(next_tail, std::memory_order_release);
    return true;
  }
  
  // Consumer: Pop an element from the queue.
  // Returns true if an element was popped, false if queue is empty.
  bool pop(T& result) {
    const size_t current_head = head_.load(std::memory_order_relaxed);
    
    if (current_head == tail_.load(std::memory_order_acquire)) {
      // Queue is empty
      return false;
    }
    
    result = buffer_[current_head];
    head_.store((current_head + 1) & (Capacity - 1), std::memory_order_release);
    return true;
  }
  
  // Check if queue is empty (approximate - for debugging only)
  bool empty() const {
    return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
  }
  
 private:
  // Ring buffer for storing elements
  T buffer_[Capacity];
  
  // Head index (consumer updates this)
  std::atomic<size_t> head_;
  
  // Tail index (producer updates this)  
  std::atomic<size_t> tail_;
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LOCKFREE_QUEUE_H