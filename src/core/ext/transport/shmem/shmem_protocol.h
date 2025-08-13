// Minimal protocol header placeholder for shmem transport.
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H

namespace grpc_shmem {

// Protocol constants
constexpr uint64_t kMagic = 0x47525043534D454Dull;  // "GRPCSMEM"
constexpr uint32_t kVersion = 1;

// Default capacities/limits
constexpr size_t kDefaultDataRingCapacityBytes = 4 * 1024 * 1024;  // 4 MiB

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H
