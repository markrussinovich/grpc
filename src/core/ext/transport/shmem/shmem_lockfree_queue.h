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
// This is a highly optimized implementation that focuses on matching
// boost's performance characteristics exactly.
template <typename T, size_t Capacity>
class SPSCQueue {
  static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of two");
  static_assert(Capacity > 1, "Capacity must be greater than 1");
  
 private:
  static constexpr size_t CAPACITY_MASK = Capacity - 1;
  
  // Put write_index at the front for producer cache locality
  std::atomic<size_t> write_index_;
  // Cache line separation to prevent false sharing
  char padding1_[64 - sizeof(std::atomic<size_t>)];
  std::atomic<size_t> read_index_;
  // More padding
  char padding2_[64 - sizeof(std::atomic<size_t>)];
  // Buffer comes last
  T buffer_[Capacity];
  
 public:
  SPSCQueue() : write_index_(0), read_index_(0) {}
  
  // Producer: Push an element to the queue.
  // Returns true if successful, false if queue is full.
  bool push(const T& item) {
    const size_t write_index = write_index_.load(std::memory_order_relaxed);
    const size_t next_write_index = (write_index + 1) & CAPACITY_MASK;
    
    // Load read_index with acquire to establish synchronization
    if (next_write_index == read_index_.load(std::memory_order_acquire)) {
      return false;
    }
    
    // Simple assignment for POD types (like Command struct)
    buffer_[write_index] = item;
    
    // Store with release to publish the write
    write_index_.store(next_write_index, std::memory_order_release);
    return true;
  }
  
  // Consumer: Pop an element from the queue.
  // Returns true if an element was popped, false if queue is empty.
  bool pop(T& result) {
    const size_t read_index = read_index_.load(std::memory_order_relaxed);
    
    // Load write_index with acquire to see published writes
    if (read_index == write_index_.load(std::memory_order_acquire)) {
      return false;
    }
    
    // Simple assignment for POD types
    result = buffer_[read_index];
    
    // Store with relaxed - only consumer modifies this
    read_index_.store((read_index + 1) & CAPACITY_MASK, std::memory_order_relaxed);
    return true;
  }
  
  // Check if queue is empty (approximate - for debugging only)
  bool empty() const {
    return read_index_.load(std::memory_order_relaxed) == 
           write_index_.load(std::memory_order_relaxed);
  }
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LOCKFREE_QUEUE_H