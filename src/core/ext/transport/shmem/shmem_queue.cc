// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "src/core/ext/transport/shmem/shmem_queue.h"

#include <algorithm>
#include <cstring>

#include <boost/interprocess/sync/scoped_lock.hpp>

namespace grpc_shmem {

namespace {
inline RingBuffer* SelectRB(ControlBlock* cb, QueueKind kind) {
  return kind == QueueKind::kC2S ? cb->c2s_queue.get() : cb->s2c_queue.get();
}
inline boost::interprocess::interprocess_mutex& SelectMutex(ControlBlock* cb,
                                                            QueueKind kind) {
  return kind == QueueKind::kC2S ? cb->c2s_mutex : cb->s2c_mutex;
}
inline boost::interprocess::interprocess_condition& SelectNotEmpty(
    ControlBlock* cb, QueueKind kind) {
  return kind == QueueKind::kC2S ? cb->c2s_cond_not_empty
                                 : cb->s2c_cond_not_empty;
}
inline boost::interprocess::interprocess_condition& SelectNotFull(
    ControlBlock* cb, QueueKind kind) {
  return kind == QueueKind::kC2S ? cb->c2s_cond_not_full
                                 : cb->s2c_cond_not_full;
}
}  // namespace

void RingWriteBlocking(ControlBlock* cb, QueueKind kind, const uint8_t* data,
                       size_t len) {
  RingBuffer* rb = SelectRB(cb, kind);
  auto& m = SelectMutex(cb, kind);
  auto& not_empty = SelectNotEmpty(cb, kind);
  auto& not_full = SelectNotFull(cb, kind);

  size_t written = 0;
  boost::interprocess::scoped_lock lock(m);
  while (written < len) {
    while (RingWritable(*rb) == 0) {
      not_full.wait(lock);
    }
    const uint64_t tail = rb->tail.load(std::memory_order_acquire);
    const uint64_t head = rb->head.load(std::memory_order_acquire);
    const uint64_t w = rb->capacity - (head - tail);
    const uint64_t pos = head % rb->capacity;
    const uint64_t chunk = std::min<uint64_t>(w, rb->capacity - pos);
    const size_t n = static_cast<size_t>(std::min<uint64_t>(chunk, len - written));
    if (n == 0) continue;
    std::memcpy(rb->buffer.get() + pos, data + written, n);
    rb->head.store(head + n, std::memory_order_release);
    written += n;
    not_empty.notify_one();
  }
}

void RingReadBlocking(ControlBlock* cb, QueueKind kind, uint8_t* out,
                      size_t len) {
  RingBuffer* rb = SelectRB(cb, kind);
  auto& m = SelectMutex(cb, kind);
  auto& not_empty = SelectNotEmpty(cb, kind);
  auto& not_full = SelectNotFull(cb, kind);

  size_t read = 0;
  boost::interprocess::scoped_lock lock(m);
  while (read < len) {
    while (RingReadable(*rb) == 0) {
      not_empty.wait(lock);
    }
    const uint64_t tail = rb->tail.load(std::memory_order_acquire);
    const uint64_t head = rb->head.load(std::memory_order_acquire);
    const uint64_t r = head - tail;
    const uint64_t pos = tail % rb->capacity;
    const uint64_t chunk = std::min<uint64_t>(r, rb->capacity - pos);
    const size_t n = static_cast<size_t>(std::min<uint64_t>(chunk, len - read));
    if (n == 0) continue;
    std::memcpy(out + read, rb->buffer.get() + pos, n);
    rb->tail.store(tail + n, std::memory_order_release);
    read += n;
    not_full.notify_one();
  }
}

}  // namespace grpc_shmem
