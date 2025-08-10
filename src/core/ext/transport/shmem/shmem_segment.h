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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H

#include <cstddef>
#include <cstdint>
#include <string>

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/shared_memory_object.hpp>

#include "src/core/ext/transport/shmem/shmem_memory.h"

namespace grpc_shmem {

constexpr uint64_t kMagic = 0x47525043534D454Dull;  // "GRPCSMEM"
constexpr uint32_t kVersion = 1;

struct SegmentConfig {
  std::string name;     // segment name (without scheme prefix)
  std::size_t size = 0; // total size in bytes
  std::size_t queue_capacity = 1 << 20;  // per-queue bytes (default 1MiB)
};

class ShmemSegment {
 public:
  static void RemoveIfExists(const std::string& name) {
    boost::interprocess::shared_memory_object::remove(name.c_str());
  }

  static ShmemSegment Create(const SegmentConfig& cfg);
  static ShmemSegment Open(const std::string& name);

  ControlBlock* control() const { return ctrl_; }
  boost::interprocess::managed_shared_memory& segment() { return *segment_; }

 private:
  ShmemSegment() = default;
  ShmemSegment(boost::interprocess::managed_shared_memory* seg, ControlBlock* cb)
      : segment_(seg), ctrl_(cb) {}

  boost::interprocess::managed_shared_memory* segment_ = nullptr;
  ControlBlock* ctrl_ = nullptr;  // lives in shared segment
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
