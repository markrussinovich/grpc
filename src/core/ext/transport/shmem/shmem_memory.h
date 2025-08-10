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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H

#include <atomic>
#include <cstdint>

#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

namespace grpc_shmem {

struct RingBuffer {
  uint64_t capacity = 0;
  std::atomic<uint64_t> head{0};  // write position
  std::atomic<uint64_t> tail{0};  // read position
  // Pointer to the start of the actual data buffer for this queue.
  boost::interprocess::offset_ptr<unsigned char> buffer{nullptr};
};

struct ControlBlock {
  uint64_t magic_number = 0x47525043534D454Dull;  // "GRPCSMEM"
  uint32_t transport_version = 1;
  std::atomic<uint32_t> server_state{0};  // 0=Down,1=Listening,2=ShuttingDown
  std::atomic<uint32_t> client_state{0};  // 0=Disc,1=Connected

  // --- Client-to-Server (C2S) Queue Synchronization ---
  boost::interprocess::interprocess_mutex c2s_mutex;
  boost::interprocess::interprocess_condition c2s_cond_not_empty;
  boost::interprocess::interprocess_condition c2s_cond_not_full;

  // --- Server-to-Client (S2C) Queue Synchronization ---
  boost::interprocess::interprocess_mutex s2c_mutex;
  boost::interprocess::interprocess_condition s2c_cond_not_empty;
  boost::interprocess::interprocess_condition s2c_cond_not_full;

  // Queues allocated within the same shared memory segment
  boost::interprocess::offset_ptr<RingBuffer> c2s_queue{nullptr};
  boost::interprocess::offset_ptr<RingBuffer> s2c_queue{nullptr};
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H
