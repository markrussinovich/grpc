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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H

#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

// For backward compatibility of simple unit tests that exercise a ring buffer
// independently, provide an alias to the new DataRingBuffer type.
using RingBuffer = DataRingBuffer;

// The new ControlBlock definition now lives in shmem_transport.h and is used
// throughout the transport; we do not duplicate it here.

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_MEMORY_H
