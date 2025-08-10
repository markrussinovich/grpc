// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H

#include <cstddef>
#include <cstdint>

#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

#include "src/core/ext/transport/shmem/shmem_memory.h"

namespace grpc_shmem {

// Identify which queue's synchronization primitives to use.
enum class QueueKind { kC2S, kS2C };

// Returns the number of bytes currently available to read.
inline uint64_t RingReadable(const RingBuffer& rb) {
  const uint64_t head = rb.head.load(std::memory_order_acquire);
  const uint64_t tail = rb.tail.load(std::memory_order_acquire);
  return head - tail;
}

// Returns the number of bytes currently available to write.
inline uint64_t RingWritable(const RingBuffer& rb) {
  return rb.capacity - RingReadable(rb);
}

// Blocking write of "len" bytes into the ring buffer.
// This function will block when the buffer is full, and will wake upon space.
// Safe for use by a single producer thread/process.
void RingWriteBlocking(ControlBlock* cb, QueueKind kind, const uint8_t* data,
                       size_t len);

// Blocking read of exactly "len" bytes from the ring buffer.
// This function will block when the buffer is empty, and will wake upon data.
// Safe for use by a single consumer thread/process.
void RingReadBlocking(ControlBlock* cb, QueueKind kind, uint8_t* out,
                      size_t len);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
