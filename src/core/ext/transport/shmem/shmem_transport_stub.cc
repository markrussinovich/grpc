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

// Temporary stub implementation of shmem transport for QPS integration.
// This will be replaced with the actual shmem transport implementation.

#include <grpc/grpc.h>
#include <grpc/support/port_platform.h>

#include "src/core/ext/transport/inproc/inproc_transport.h"

extern "C" grpc_channel* grpc_shmem_channel_create(
    grpc_server* server, const grpc_channel_args* args, void* reserved) {
  // Temporary stub: forward to inproc for now
  // TODO: Replace with actual shmem transport implementation
  return grpc_inproc_channel_create(server, args, reserved);
}
