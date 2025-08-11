// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/core/ext/transport/shmem/shmem_protocol.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"

namespace grpc_shmem {

// Size of the serialized header in bytes (fixed layout, little-endian fields).
constexpr size_t kSerializedHeaderSize = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t);

// Serialize a FrameHeader into a 12-byte little-endian buffer.
void SerializeHeaderLE(const FrameHeader& hdr, uint8_t out[kSerializedHeaderSize]);

// Parse a 12-byte little-endian header buffer into FrameHeader.
FrameHeader ParseHeaderLE(const uint8_t in[kSerializedHeaderSize]);

// Blocking write of a complete frame (header then payload) to the queue.
// 'hdr.frame_size' must equal kSerializedHeaderSize + payload_size.
void WriteFrame(ControlBlock* cb, QueueKind kind, const FrameHeader& hdr, const uint8_t* payload, size_t payload_size);

// Blocking read of one complete frame from the queue. Returns the payload bytes.
// Writes the parsed header into out_hdr.
std::vector<uint8_t> ReadFrame(ControlBlock* cb, QueueKind kind, FrameHeader* out_hdr);

// Minimal metadata encoding helpers for the unary path (WIP):
// Encode the HTTP path in a simple little-endian length-prefixed format.
std::vector<uint8_t> EncodeInitialMdPath(const std::string& path);
// Decode the HTTP path from the bytes encoded by EncodeInitialMdPath.
std::string DecodeInitialMdPath(const std::vector<uint8_t>& bytes);
// Encode a trailing status code (gRPC status integer) as little-endian uint32.
std::vector<uint8_t> EncodeTrailingStatus(uint32_t status_code);
// Decode a trailing status code from little-endian uint32.
uint32_t DecodeTrailingStatus(const std::vector<uint8_t>& bytes);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
