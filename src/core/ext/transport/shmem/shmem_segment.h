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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <string>

#include "src/core/ext/transport/shmem/shmem_protocol.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/ext/transport/shmem/shmem_semaphore.h"

namespace grpc_shmem {

struct SegmentConfig {
  std::string name;
  std::string server_name;  // for cross-process semaphore naming
  std::size_t size = 0;  // total shared memory size in bytes
  std::size_t data_ring_capacity =
      kDefaultDataRingCapacityBytes;  // per-direction
};

class ShmemSegment {
 public:
  ShmemSegment() = default;
  ~ShmemSegment() { Unmap(); }
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
  
  // Remove named semaphores for cross-process cleanup
  static void RemoveNamedSemaphores(const std::string& server_name);

  // Create a new segment and initialize ControlBlock and queues.
  static ShmemSegment Create(const SegmentConfig& cfg);

  // Open an existing segment; verify control block and mark client connected.
  static ShmemSegment Open(const std::string& name);

  // Pointer into shared segment; valid while this ShmemSegment is alive.
  ControlBlock* control() const { return control_; }
  
  // Get the segment name for debugging
  const std::string& name() const { return name_; }
  
  // Control whether this segment should unlink on destruction
  void SetUnlinkOnDestroy(bool should_unlink) { should_unlink_on_destroy_ = should_unlink; }

 private:
  ShmemSegment(std::string name, void* base, size_t size, ControlBlock* cb, int fd)
      : name_(std::move(name)), base_(base), size_(size), control_(cb), fd_(fd) {}

  void MoveFrom(ShmemSegment&& other) {
    name_ = std::move(other.name_);
    base_ = other.base_; other.base_ = nullptr;  // FIX: Clear base_ to prevent double unmap
    size_ = other.size_; other.size_ = 0;        // FIX: Clear size_ for consistency  
    control_ = other.control_; other.control_ = nullptr;
    fd_ = other.fd_; other.fd_ = -1;
    should_unlink_on_destroy_ = other.should_unlink_on_destroy_;
  }

  static void InitQueues(void* base, size_t size, ControlBlock* cb,
                         std::size_t data_ring_capacity,
                         const std::string& server_name = "");

  static int CreateFd(const std::string& name, size_t size, std::string* created_name);
  static int OpenFd(const std::string& name, size_t* size_out);
  static void* Map(int fd, size_t size);
  void Unmap();

  std::string name_;
  void* base_ = nullptr;
  size_t size_ = 0;
  ControlBlock* control_ = nullptr;  // points into segment_
  int fd_ = -1;
  bool should_unlink_on_destroy_ = true;  // Controls whether destructor calls shm_unlink
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
