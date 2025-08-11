// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "src/core/ext/transport/shmem/shmem_framer.h"

#include <cstring>

namespace grpc_shmem {

namespace {
inline void Store32LE(uint8_t* p, uint32_t v) {
  p[0] = static_cast<uint8_t>(v & 0xFF);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
  p[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
  p[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}
inline void Store16LE(uint8_t* p, uint16_t v) {
  p[0] = static_cast<uint8_t>(v & 0xFF);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
}
inline uint32_t Load32LE(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) |
         (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}
inline uint16_t Load16LE(const uint8_t* p) {
  return static_cast<uint16_t>(p[0]) |
         static_cast<uint16_t>(static_cast<uint16_t>(p[1]) << 8);
}
}  // namespace

void SerializeHeaderLE(const FrameHeader& hdr, uint8_t out[kSerializedHeaderSize]) {
  Store32LE(out + 0, hdr.frame_size);
  Store32LE(out + 4, hdr.stream_id);
  out[8] = static_cast<uint8_t>(hdr.type);
  out[9] = static_cast<uint8_t>(hdr.flags);
  Store16LE(out + 10, hdr.reserved);
}

FrameHeader ParseHeaderLE(const uint8_t in[kSerializedHeaderSize]) {
  FrameHeader h{};
  h.frame_size = Load32LE(in + 0);
  h.stream_id = Load32LE(in + 4);
  h.type = static_cast<FrameType>(in[8]);
  h.flags = static_cast<FrameFlags>(in[9]);
  h.reserved = Load16LE(in + 10);
  return h;
}

void WriteFrame(ControlBlock* cb, QueueKind kind, const FrameHeader& hdr, const uint8_t* payload, size_t payload_size) {
  // Safety check: expected size
  (void)payload_size;
  uint8_t header_buf[kSerializedHeaderSize];
  SerializeHeaderLE(hdr, header_buf);
  RingWriteBlocking(cb, kind, header_buf, kSerializedHeaderSize);
  if (hdr.frame_size > kSerializedHeaderSize) {
    RingWriteBlocking(cb, kind, payload, hdr.frame_size - kSerializedHeaderSize);
  }
}

std::vector<uint8_t> ReadFrame(ControlBlock* cb, QueueKind kind, FrameHeader* out_hdr) {
  uint8_t header_buf[kSerializedHeaderSize];
  RingReadBlocking(cb, kind, header_buf, kSerializedHeaderSize);
  *out_hdr = ParseHeaderLE(header_buf);
  const size_t payload_size = (*out_hdr).frame_size > kSerializedHeaderSize ? (*out_hdr).frame_size - kSerializedHeaderSize : 0;
  std::vector<uint8_t> payload(payload_size);
  if (payload_size > 0) {
    RingReadBlocking(cb, kind, payload.data(), payload_size);
  }
  return payload;
}

}  // namespace grpc_shmem
