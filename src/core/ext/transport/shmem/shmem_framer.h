// Framing and slice helpers for shmem transport.
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H

#include <cstdint>
#include <string>
#include <vector>

#include "include/grpc/slice.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

// Create a grpc_slice that references memory inside the DataRingBuffer without copying.
// The slice destructor advances rb->tail by size to free space.
grpc_slice MakeSliceFromRing(DataRingBuffer* rb, uint64_t offset, uint32_t size);

// Simple metadata key/value representation.
struct KVPair {
	std::string key;
	std::string value;
};

// Serialize key/values into a compact little-endian payload:
// [u16 count] { [u16 key_len][key bytes][u32 val_len][val bytes] }*
std::vector<uint8_t> SerializeMetadataKVs(const std::vector<KVPair>& kvs);

// Deserialize the above format; returns empty vector on parse error.
std::vector<KVPair> DeserializeMetadataKVs(const uint8_t* bytes, size_t len);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
