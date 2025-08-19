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

#include "src/core/ext/transport/shmem/shmem_framer.h"

#include <cstring>

namespace grpc_shmem {

namespace {
struct TailRelease {
  DataRingBuffer* rb;
  uint32_t size;
};

void ReleaseTail(void* ud) {
  auto* tr = static_cast<TailRelease*>(ud);
  if (tr != nullptr && tr->rb != nullptr && tr->size > 0) {
    tr->rb->tail.fetch_add(tr->size, std::memory_order_release);
  }
  delete tr;
}
}  // namespace

grpc_slice MakeSliceFromRing(DataRingBuffer* rb, void* segment_base,
                             uint64_t offset, uint32_t size) {
  // Note: we assume caller guaranteed contiguous region [offset, offset+size)
  // within the ring bounds (offset+size <= capacity).
  unsigned char* ptr = rb->GetBuffer(segment_base) + offset;
  auto* ud = new TailRelease{rb, size};
  return grpc_slice_new_with_user_data(ptr, size, &ReleaseTail, ud);
}

static inline void WriteU16LE(uint16_t v, uint8_t* p) {
  p[0] = static_cast<uint8_t>(v & 0xff);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xff);
}
static inline void WriteU32LE(uint32_t v, uint8_t* p) {
  p[0] = static_cast<uint8_t>(v & 0xff);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xff);
  p[2] = static_cast<uint8_t>((v >> 16) & 0xff);
  p[3] = static_cast<uint8_t>((v >> 24) & 0xff);
}
static inline uint16_t ReadU16LE(const uint8_t* p) {
  return static_cast<uint16_t>(p[0] | (static_cast<uint16_t>(p[1]) << 8));
}
static inline uint32_t ReadU32LE(const uint8_t* p) {
  return static_cast<uint32_t>(p[0] | (static_cast<uint32_t>(p[1]) << 8) |
                               (static_cast<uint32_t>(p[2]) << 16) |
                               (static_cast<uint32_t>(p[3]) << 24));
}

std::vector<uint8_t> SerializeMetadataKVs(const std::vector<KVPair>& kvs) {
  size_t total = 2;  // count
  for (const auto& kv : kvs) {
    total += 2 + kv.key.size();
    total += 4 + kv.value.size();
  }
  std::vector<uint8_t> out(total);
  WriteU16LE(static_cast<uint16_t>(kvs.size()), out.data());
  size_t o = 2;
  for (const auto& kv : kvs) {
    WriteU16LE(static_cast<uint16_t>(kv.key.size()), out.data() + o);
    o += 2;
    if (!kv.key.empty()) {
      std::memcpy(out.data() + o, kv.key.data(), kv.key.size());
      o += kv.key.size();
    }
    WriteU32LE(static_cast<uint32_t>(kv.value.size()), out.data() + o);
    o += 4;
    if (!kv.value.empty()) {
      std::memcpy(out.data() + o, kv.value.data(), kv.value.size());
      o += kv.value.size();
    }
  }
  return out;
}

std::vector<KVPair> DeserializeMetadataKVs(const uint8_t* bytes, size_t len) {
  std::vector<KVPair> out;
  if (len < 2) return out;
  size_t o = 0;
  const uint16_t n = ReadU16LE(bytes + o);
  o += 2;
  for (uint16_t i = 0; i < n; ++i) {
    if (o + 2 > len) return {};
    const uint16_t klen = ReadU16LE(bytes + o);
    o += 2;
    if (o + klen > len) return {};
    std::string key(reinterpret_cast<const char*>(bytes + o), klen);
    o += klen;
    if (o + 4 > len) return {};
    const uint32_t vlen = ReadU32LE(bytes + o);
    o += 4;
    if (o + vlen > len) return {};
    std::string val(reinterpret_cast<const char*>(bytes + o), vlen);
    o += vlen;
    out.push_back(KVPair{std::move(key), std::move(val)});
  }
  return out;
}

}  // namespace grpc_shmem
