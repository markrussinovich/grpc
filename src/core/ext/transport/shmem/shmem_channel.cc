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

// Shared-memory (shmem) in-process-style channel factory.
// This follows the same structure as grpc_inproc_channel_create:
//  * build a client/server transport pair
//  * Server::SetupTransport(server_transport, ...)
//  * ChannelCreate("shmem", ..., GRPC_CLIENT_DIRECT_CHANNEL, client_transport)
//
// References (inproc pattern we mirror):
//   - MakeInprocChannel / grpc_inproc_channel_create implementation. [1][2]
//
// [1] MakeInprocChannel / MakeInProcessTransportPair:
//     src/core/ext/transport/inproc/inproc_transport.cc lines ~L16-L19
// [2] grpc_inproc_channel_create wrapper:
//     src/core/ext/transport/inproc/inproc_transport.cc lines ~L19-L20

#include <grpc/grpc.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/core/client_channel/direct_channel.h"  // DirectChannel (promise stack)
#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"  // MakeShmemTransportPair
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/surface/channel_create.h"
#include "src/core/lib/surface/lame_client.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/server/server.h"
#include "src/core/util/ref_counted_ptr.h"

namespace grpc_core {
namespace {

// Matches the lame-channel fallback used by inproc for error paths. [1]
static RefCountedPtr<Channel> MakeLameChannel(absl::string_view why,
                                              absl::Status error) {
  intptr_t integer;
  grpc_status_code status = GRPC_STATUS_INTERNAL;
  if (grpc_error_get_int(error, StatusIntProperty::kRpcStatus, &integer)) {
    status = static_cast<grpc_status_code>(integer);
  }
  return RefCountedPtr<Channel>(Channel::FromC(grpc_lame_client_channel_create(
      /*target=*/nullptr, status, std::string(why).c_str())));
}

// Exact analog of MakeInprocChannel(...) but for shmem. [1]
static RefCountedPtr<Channel> MakeShmemChannel(
    Server* server, ChannelArgs client_channel_args) {
  // 1) Build the transport pair using the server's ChannelArgs.
  auto transports = MakeShmemTransportPair(server->channel_args());
  auto client_transport = std::move(transports.first);
  auto server_transport = std::move(transports.second);

  // 2) Hand the server half to the server.
  auto error = server->SetupTransport(
      server_transport.get(),
      /*accept_stream_fn=*/nullptr,
      // Inproc code removes these two args; we do the same for symmetry. [1]
      server->channel_args()
          .Remove(GRPC_ARG_MAX_CONNECTION_IDLE_MS)
          .Remove(GRPC_ARG_MAX_CONNECTION_AGE_MS),
      /*socket_node=*/nullptr);
  if (!error.ok()) {
    // DEBUG: SetupTransport failed - this means server transport was destroyed
    // and SetCallDestination() will never be called, causing server_calls_created=0
    return MakeLameChannel("Failed to create server channel", std::move(error));
  }
  // SetupTransport takes ownership through the vtable; don't delete it here.
  (void)server_transport.release();

  // 3) Create a **promise-based direct channel** bound to our client transport.
  //    Use ChannelCreate with GRPC_CLIENT_DIRECT_CHANNEL and explicit transport
  //    parameter to ensure proper transport attachment (following inproc pattern).
  auto channel_result = ChannelCreate(
      /*target=*/"shmem",
      client_channel_args.Set(GRPC_ARG_DEFAULT_AUTHORITY, "shmem.authority")
          .Set(GRPC_ARG_USE_V3_STACK, true),
      GRPC_CLIENT_DIRECT_CHANNEL,
      /*optional_transport=*/client_transport.release());
  if (!channel_result.ok()) {
    return MakeLameChannel("Failed to create direct channel",
                           channel_result.status());
  }
  return std::move(*channel_result);
}

}  // namespace
}  // namespace grpc_core

extern "C" grpc_channel* grpc_shmem_channel_create(
    grpc_server* server, const grpc_channel_args* args, void* /*reserved*/) {
  // Match inproc: ensure callback/exec contexts exist while we build channel.
  // [2]
  grpc_core::ExecCtx exec_ctx;
  // Use the same channel-arg preconditioning as inproc before creating channel.
  // [2]
  auto client_args = grpc_core::CoreConfiguration::Get()
                         .channel_args_preconditioning()
                         .PreconditionChannelArgs(args);
  auto ch = grpc_core::MakeShmemChannel(grpc_core::Server::FromC(server),
                                        std::move(client_args));
  return ch.release()->c_ptr();
}
