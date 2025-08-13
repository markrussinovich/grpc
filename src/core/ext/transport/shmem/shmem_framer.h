// Framing and slice helpers for shmem transport.
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H

#include <cstdint>

#include "include/grpc/slice.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

// Create a grpc_slice that references memory inside the DataRingBuffer without copying.
// The slice destructor advances rb->tail by size to free space.
grpc_slice MakeSliceFromRing(DataRingBuffer* rb, uint64_t offset, uint32_t size);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_FRAMER_H
