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
#include <thread>
#include <chrono>

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

#ifndef GRPC_SHMEM_SPSC_LOCKFREE
#define GRPC_SHMEM_SPSC_LOCKFREE 0  // Keep mutex version for better blocking
#endif

namespace {
inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  asm volatile("pause" ::: "memory");
#else
  // Fallback noop
#endif
}

inline void backoff_wait(int iter) {
  if (iter < 64) {
    cpu_relax();
  } else if (iter < 256) {
    std::this_thread::yield();
  } else {
    std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
  }
}
}  // namespace

void RingWriteBlocking(ControlBlock* cb, QueueKind kind, const uint8_t* data,
                       size_t len) {
  RingBuffer* rb = SelectRB(cb, kind);
  size_t written = 0;
#if GRPC_SHMEM_SPSC_LOCKFREE
  // Aggressive lock-free SPSC with minimal spinning
  while (written < len) {
    const uint64_t head = rb->head.load(std::memory_order_relaxed);
    const uint64_t tail = rb->tail.load(std::memory_order_acquire);
    const uint64_t available = rb->capacity - (head - tail);
    
    if (available == 0) {
      // Tight spin for a few iterations, then yield
      for (int i = 0; i < 8; ++i) {
        cpu_relax();
        if (rb->tail.load(std::memory_order_acquire) != tail) break;
      }
      continue;
    }
    
    const uint64_t pos = head % rb->capacity;
    const uint64_t to_end = rb->capacity - pos;
    const size_t chunk = std::min({available, to_end, len - written});
    
    std::memcpy(rb->buffer.get() + pos, data + written, chunk);
    rb->head.store(head + chunk, std::memory_order_release);
    written += chunk;
  }
#else
  auto& m = SelectMutex(cb, kind);
  auto& not_empty = SelectNotEmpty(cb, kind);
  auto& not_full = SelectNotFull(cb, kind);
  boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(m);
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
#endif
}

void RingReadBlocking(ControlBlock* cb, QueueKind kind, uint8_t* out,
                      size_t len) {
  RingBuffer* rb = SelectRB(cb, kind);
  size_t read = 0;
#if GRPC_SHMEM_SPSC_LOCKFREE
  // Aggressive lock-free SPSC with minimal spinning  
  while (read < len) {
    const uint64_t head = rb->head.load(std::memory_order_acquire);
    const uint64_t tail = rb->tail.load(std::memory_order_relaxed);
    const uint64_t available = head - tail;
    
    if (available == 0) {
      // Tight spin for a few iterations, then yield
      for (int i = 0; i < 8; ++i) {
        cpu_relax();
        if (rb->head.load(std::memory_order_acquire) != head) break;
      }
      continue;
    }
    
    const uint64_t pos = tail % rb->capacity;
    const uint64_t to_end = rb->capacity - pos;
    const size_t chunk = std::min({available, to_end, len - read});
    
    std::memcpy(out + read, rb->buffer.get() + pos, chunk);
    rb->tail.store(tail + chunk, std::memory_order_release);
    read += chunk;
  }
#else
  auto& m = SelectMutex(cb, kind);
  auto& not_empty = SelectNotEmpty(cb, kind);
  auto& not_full = SelectNotFull(cb, kind);
  boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(m);
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
#endif
}

}  // namespace grpc_shmem
