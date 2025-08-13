// Shared memory segment management for shmem transport (Phase 2).
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H

#include <cstddef>
#include <string>

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/shared_memory_object.hpp>

#include "src/core/ext/transport/shmem/shmem_protocol.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

struct SegmentConfig {
  std::string name;
  std::size_t size = 0;  // total shared memory size in bytes
  std::size_t data_ring_capacity = kDefaultDataRingCapacityBytes;  // per-direction
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

  static void RemoveIfExists(const std::string& name) {
    boost::interprocess::shared_memory_object::remove(name.c_str());
  }

  // Create a new segment and initialize ControlBlock and queues.
  static ShmemSegment Create(const SegmentConfig& cfg);

  // Open an existing segment; verify control block and mark client connected.
  static ShmemSegment Open(const std::string& name);

  // Pointer into shared segment; valid while this ShmemSegment is alive.
  ControlBlock* control() const { return control_; }

 private:
  explicit ShmemSegment(std::string name, std::unique_ptr<boost::interprocess::managed_shared_memory> seg,
                        ControlBlock* cb)
      : name_(std::move(name)), segment_(std::move(seg)), control_(cb) {}

  void MoveFrom(ShmemSegment&& other) {
    name_ = std::move(other.name_);
    segment_ = std::move(other.segment_);
    control_ = other.control_;
    other.control_ = nullptr;
  }

  static void InitQueues(boost::interprocess::managed_shared_memory& seg, ControlBlock* cb,
                         std::size_t data_ring_capacity);

  std::string name_;
  std::unique_ptr<boost::interprocess::managed_shared_memory> segment_;
  ControlBlock* control_ = nullptr;  // points into segment_
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEGMENT_H
