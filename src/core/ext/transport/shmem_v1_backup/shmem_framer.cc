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
  
  // Optimization: For small frames (≤ 256 bytes), combine header and payload into single ring write
  const size_t payload_bytes = hdr.frame_size > kSerializedHeaderSize ? hdr.frame_size - kSerializedHeaderSize : 0;
  if (payload_bytes <= 256) {
    // Use stack buffer for small combined writes
    uint8_t combined_buf[kSerializedHeaderSize + 256];
    std::memcpy(combined_buf, header_buf, kSerializedHeaderSize);
    if (payload_bytes > 0) {
      std::memcpy(combined_buf + kSerializedHeaderSize, payload, payload_bytes);
    }
    RingWriteBlocking(cb, kind, combined_buf, kSerializedHeaderSize + payload_bytes);
  } else {
    // Use separate writes for large frames
    RingWriteBlocking(cb, kind, header_buf, kSerializedHeaderSize);
    if (payload_bytes > 0) {
      RingWriteBlocking(cb, kind, payload, payload_bytes);
    }
  }
}

std::vector<uint8_t> ReadFrame(ControlBlock* cb, QueueKind kind, FrameHeader* out_hdr) {
  uint8_t header_buf[kSerializedHeaderSize];
  RingReadBlocking(cb, kind, header_buf, kSerializedHeaderSize);
  *out_hdr = ParseHeaderLE(header_buf);
  const size_t payload_size = (*out_hdr).frame_size > kSerializedHeaderSize ? (*out_hdr).frame_size - kSerializedHeaderSize : 0;
  std::vector<uint8_t> payload(payload_size);
  if (payload_size > 0) {
    // Optimization: For small payloads (≤256 bytes), we might want to read 
    // header+payload in one operation, but that requires predicting frame size.
    // For now, keep separate read since we need header to know payload size.
    RingReadBlocking(cb, kind, payload.data(), payload_size);
  }
  return payload;
}

std::vector<uint8_t> EncodeMetadataKVs(const std::vector<grpc_shmem::KVPair>& kvs) {
  // Compute size: 2 bytes count + sum(2 + key + 4 + val)
  size_t size = 2;
  for (const auto& kv : kvs) {
    size += 2 + kv.key.size();
    size += 4 + kv.value.size();
  }
  std::vector<uint8_t> out(size);
  // Write count
  uint16_t count = static_cast<uint16_t>(kvs.size());
  out[0] = static_cast<uint8_t>(count & 0xFF);
  out[1] = static_cast<uint8_t>((count >> 8) & 0xFF);
  size_t o = 2;
  for (const auto& kv : kvs) {
    uint16_t klen = static_cast<uint16_t>(kv.key.size());
    out[o + 0] = static_cast<uint8_t>(klen & 0xFF);
    out[o + 1] = static_cast<uint8_t>((klen >> 8) & 0xFF);
    o += 2;
    if (klen) {
      std::memcpy(out.data() + o, kv.key.data(), klen);
      o += klen;
    }
    uint32_t vlen = static_cast<uint32_t>(kv.value.size());
    out[o + 0] = static_cast<uint8_t>(vlen & 0xFF);
    out[o + 1] = static_cast<uint8_t>((vlen >> 8) & 0xFF);
    out[o + 2] = static_cast<uint8_t>((vlen >> 16) & 0xFF);
    out[o + 3] = static_cast<uint8_t>((vlen >> 24) & 0xFF);
    o += 4;
    if (vlen) {
      std::memcpy(out.data() + o, kv.value.data(), vlen);
      o += vlen;
    }
  }
  return out;
}

std::vector<grpc_shmem::KVPair> DecodeMetadataKVs(const std::vector<uint8_t>& bytes) {
  std::vector<grpc_shmem::KVPair> out;
  if (bytes.size() < 2) return out;
  uint16_t count = static_cast<uint16_t>(bytes[0]) |
                   static_cast<uint16_t>(static_cast<uint16_t>(bytes[1]) << 8);
  size_t o = 2;
  out.reserve(count);
  for (uint16_t i = 0; i < count; ++i) {
    if (o + 2 > bytes.size()) { out.clear(); return out; }
    uint16_t klen = static_cast<uint16_t>(bytes[o]) |
                    static_cast<uint16_t>(static_cast<uint16_t>(bytes[o+1]) << 8);
    o += 2;
    if (o + klen > bytes.size()) { out.clear(); return out; }
    std::string key;
    key.resize(klen);
    if (klen) std::memcpy(key.data(), bytes.data() + o, klen);
    o += klen;
    if (o + 4 > bytes.size()) { out.clear(); return out; }
    uint32_t vlen = static_cast<uint32_t>(bytes[o]) |
                    (static_cast<uint32_t>(bytes[o+1]) << 8) |
                    (static_cast<uint32_t>(bytes[o+2]) << 16) |
                    (static_cast<uint32_t>(bytes[o+3]) << 24);
    o += 4;
    if (o + vlen > bytes.size()) { out.clear(); return out; }
    std::string val;
    val.resize(vlen);
    if (vlen) std::memcpy(val.data(), bytes.data() + o, vlen);
    o += vlen;
    out.push_back(grpc_shmem::KVPair{std::move(key), std::move(val)});
  }
  return out;
}

std::vector<uint8_t> EncodeInitialMdPath(const std::string& path) {
  // Simple 32-bit little-endian length + bytes
  std::vector<uint8_t> out(4 + path.size());
  const uint32_t n = static_cast<uint32_t>(path.size());
  out[0] = static_cast<uint8_t>(n & 0xFF);
  out[1] = static_cast<uint8_t>((n >> 8) & 0xFF);
  out[2] = static_cast<uint8_t>((n >> 16) & 0xFF);
  out[3] = static_cast<uint8_t>((n >> 24) & 0xFF);
  if (!path.empty()) {
    std::memcpy(out.data() + 4, path.data(), path.size());
  }
  return out;
}

std::string DecodeInitialMdPath(const std::vector<uint8_t>& bytes) {
  if (bytes.size() < 4) return std::string();
  uint32_t n = static_cast<uint32_t>(bytes[0]) |
               (static_cast<uint32_t>(bytes[1]) << 8) |
               (static_cast<uint32_t>(bytes[2]) << 16) |
               (static_cast<uint32_t>(bytes[3]) << 24);
  if (bytes.size() < 4u + n) return std::string();
  return std::string(reinterpret_cast<const char*>(bytes.data() + 4), n);
}

std::vector<uint8_t> EncodeTrailingStatus(uint32_t status_code) {
  std::vector<uint8_t> out(4);
  out[0] = static_cast<uint8_t>(status_code & 0xFF);
  out[1] = static_cast<uint8_t>((status_code >> 8) & 0xFF);
  out[2] = static_cast<uint8_t>((status_code >> 16) & 0xFF);
  out[3] = static_cast<uint8_t>((status_code >> 24) & 0xFF);
  return out;
}

uint32_t DecodeTrailingStatus(const std::vector<uint8_t>& bytes) {
  if (bytes.size() < 4) return 0;
  return static_cast<uint32_t>(bytes[0]) |
         (static_cast<uint32_t>(bytes[1]) << 8) |
         (static_cast<uint32_t>(bytes[2]) << 16) |
         (static_cast<uint32_t>(bytes[3]) << 24);
}

}  // namespace grpc_shmem
