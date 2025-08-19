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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LEGACY_TRANSPORT_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_LEGACY_TRANSPORT_H

#include <grpc/grpc.h>
#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/surface/channel.h"
#include "src/core/server/server.h"
#include "src/core/util/ref_counted_ptr.h"

namespace grpc_core {

// Create a legacy (filter-stack) shmem channel for hosting legacy filters.
// This is used as a fallback when v3 channel creation fails due to legacy-only filters.
RefCountedPtr<Channel> MakeLegacyShmemChannel(Server* server, const ChannelArgs& args);

}  // namespace grpc_core

// Legacy shmem channel creation function (extern "C" for compatibility)
extern "C" grpc_channel* grpc_legacy_shmem_channel_create(grpc_server* server,
                                                         const grpc_channel_args* args,
                                                         void* reserved);

#endif
