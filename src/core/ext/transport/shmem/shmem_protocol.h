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
