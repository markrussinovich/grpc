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

#include "src/core/ext/transport/shmem/shmem_segment.h"

#include <cstring>

#include <boost/interprocess/allocators/allocator.hpp>

namespace bip = boost::interprocess;

namespace grpc_shmem {

namespace {
constexpr const char* kControlBlockName = "grpc_shmem_control";
constexpr const char* kC2SBufName = "grpc_shmem_c2s_buf";
constexpr const char* kS2CBufName = "grpc_shmem_s2c_buf";
}  // namespace

ShmemSegment ShmemSegment::Create(const SegmentConfig& cfg) {
  // Create segment
  auto* seg = new bip::managed_shared_memory(bip::create_only, cfg.name.c_str(), cfg.size);
  // Construct ControlBlock at a known name
  ControlBlock* cb = seg->construct<ControlBlock>(kControlBlockName)();
  cb->magic_number = kMagic;
  cb->transport_version = kVersion;
  cb->server_state.store(1);  // Listening
  cb->client_state.store(0);

  // Allocate ring buffers and backing storage inside segment
  cb->c2s_queue = seg->construct<RingBuffer>("c2s_queue")();
  cb->s2c_queue = seg->construct<RingBuffer>("s2c_queue")();

  cb->c2s_queue->capacity = cfg.queue_capacity;
  cb->s2c_queue->capacity = cfg.queue_capacity;

  // Allocate raw buffers
  cb->c2s_queue->buffer = seg->construct<unsigned char>(kC2SBufName)[cfg.queue_capacity]();
  cb->s2c_queue->buffer = seg->construct<unsigned char>(kS2CBufName)[cfg.queue_capacity]();

  return ShmemSegment(seg, cb);
}

ShmemSegment ShmemSegment::Open(const std::string& name) {
  auto* seg = new bip::managed_shared_memory(bip::open_only, name.c_str());
  ControlBlock* cb = seg->find<ControlBlock>(kControlBlockName).first;
  if (cb == nullptr || cb->magic_number != kMagic || cb->transport_version != kVersion) {
    // Invalid/unknown segment; throw to signal error to caller
    throw std::runtime_error("Invalid shmem segment");
  }
  cb->client_state.store(1);
  return ShmemSegment(seg, cb);
}

}  // namespace grpc_shmem
