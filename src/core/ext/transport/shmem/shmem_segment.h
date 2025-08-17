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

// Shared memory segment management for shmem transport (Phase 2).
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstddef>
#include <string>

#include "src/core/ext/transport/shmem/shmem_protocol.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

struct SegmentConfig {
  std::string name;
  std::size_t size = 0;  // total shared memory size in bytes
  std::size_t data_ring_capacity =
      kDefaultDataRingCapacityBytes;  // per-direction
};

class ShmemSegment {
 public:
  ShmemSegment() = default;
  ~ShmemSegment() = default;
  ShmemSegment(ShmemSegment&& other) noexcept { MoveFrom(std::move(other)); }
  ShmemSegment& operator=(ShmemSegment&& other) noexcept {
    if (this != &other) MoveFrom(std::move(other));
    return *this;
  }
  ShmemSegment(const ShmemSegment&) = delete;
  ShmemSegment& operator=(const ShmemSegment&) = delete;

  // Remove the shared memory segment for this process-local name.
  // Note: To avoid cross-test interference, the implementation removes a
  // process-suffixed name; see shmem_segment.cc for details.
  static void RemoveIfExists(const std::string& name);

  // Create a new segment and initialize ControlBlock and queues.
  static ShmemSegment Create(const SegmentConfig& cfg);

  // Open an existing segment; verify control block and mark client connected.
  static ShmemSegment Open(const std::string& name);

  // Pointer into shared segment; valid while this ShmemSegment is alive.
  ControlBlock* control() const { return control_; }

 private:
  explicit ShmemSegment(
      std::string name,
      std::unique_ptr<boost::interprocess::managed_shared_memory> seg,
      ControlBlock* cb)
      : name_(std::move(name)), segment_(std::move(seg)), control_(cb) {}

  void MoveFrom(ShmemSegment&& other) {
    name_ = std::move(other.name_);
    segment_ = std::move(other.segment_);
    control_ = other.control_;
    other.control_ = nullptr;
  }

  static void InitQueues(boost::interprocess::managed_shared_memory& seg,
                         ControlBlock* cb, std::size_t data_ring_capacity);

  std::string name_;
  std::unique_ptr<boost::interprocess::managed_shared_memory> segment_;
  ControlBlock* control_ = nullptr;  // points into segment_
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
