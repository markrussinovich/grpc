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
#include "absl/strings/match.h"
#include "src/core/client_channel/direct_channel.h"  // DirectChannel (promise stack)
#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"  // MakeShmemTransportPair
#include "src/core/ext/transport/shmem/shmem_legacy_transport.h"  // legacy shim
#include "src/core/lib/iomgr/error.h"
#include "src/core/lib/promise/promise.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/surface/channel_create.h"
#include "src/core/lib/surface/lame_client.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/server/server.h"
#include "src/core/util/ref_counted_ptr.h"

namespace grpc_core {
namespace {

// Map absl::Status -> grpc_status_code, honoring kRpcStatus if present.
static grpc_status_code MapToGrpcStatus(const absl::Status& st) {
  intptr_t integer;
  if (grpc_error_get_int(st, StatusIntProperty::kRpcStatus, &integer)) {
    return static_cast<grpc_status_code>(integer);
  }
  using C = absl::StatusCode;
  switch (st.code()) {
    case C::kOk:                return GRPC_STATUS_OK;
    case C::kCancelled:         return GRPC_STATUS_CANCELLED;
    case C::kUnknown:           return GRPC_STATUS_UNKNOWN;
    case C::kInvalidArgument:   return GRPC_STATUS_INVALID_ARGUMENT;
    case C::kDeadlineExceeded:  return GRPC_STATUS_DEADLINE_EXCEEDED;
    case C::kNotFound:          return GRPC_STATUS_NOT_FOUND;
    case C::kAlreadyExists:     return GRPC_STATUS_ALREADY_EXISTS;
    case C::kPermissionDenied:  return GRPC_STATUS_PERMISSION_DENIED;
    case C::kResourceExhausted: return GRPC_STATUS_RESOURCE_EXHAUSTED;
    case C::kFailedPrecondition:return GRPC_STATUS_FAILED_PRECONDITION;
    case C::kAborted:           return GRPC_STATUS_ABORTED;
    case C::kOutOfRange:        return GRPC_STATUS_OUT_OF_RANGE;
    case C::kUnimplemented:     return GRPC_STATUS_UNIMPLEMENTED;
    case C::kInternal:          return GRPC_STATUS_INTERNAL;
    case C::kUnavailable:       return GRPC_STATUS_UNAVAILABLE;
    case C::kDataLoss:          return GRPC_STATUS_DATA_LOSS;
    case C::kUnauthenticated:   return GRPC_STATUS_UNAUTHENTICATED;
    default:                    return GRPC_STATUS_INTERNAL;
  }
}

// Build a lame channel using the mapped status + the original error message.
static RefCountedPtr<Channel> MakeLameChannelFromStatus(const absl::Status& st,
                                                        absl::string_view fallback_why) {
  const grpc_status_code code = MapToGrpcStatus(st);
  const std::string msg = st.message().empty()
                              ? std::string(fallback_why)
                              : std::string(st.message());
  return RefCountedPtr<Channel>(
      Channel::FromC(grpc_lame_client_channel_create(/*target=*/nullptr,
                                                     code, msg.c_str())));
}

}  // namespace

// Exact analog of MakeInprocChannel(...) but for shmem. [1]
RefCountedPtr<Channel> MakeShmemChannel(
    Server* server, ChannelArgs client_channel_args) {
  // 1) Build the transport pair using both server and client ChannelArgs.
  auto transports = MakeShmemTransportPair(server->channel_args(), client_channel_args);
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
    return MakeLameChannelFromStatus(error, "server transport setup failed");
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
    const std::string msg = std::string(channel_result.status().message());
    // Fallback: legacy-only filter present (v3 cannot host it).
    // Detect the canonical error and build a legacy channel so the legacy filter
    // can run and report its intended status (e.g., PERMISSION_DENIED: "access denied").
    if (msg.find("no v3-callstack vtable") != std::string::npos) {
      return MakeLegacyShmemChannel(server, client_channel_args);
    }
    return MakeLameChannelFromStatus(channel_result.status(),
                                     "direct channel creation failed");
  }
  return std::move(*channel_result);
}
}  // namespace grpc_core

extern "C" grpc_channel* grpc_shmem_channel_create(
    grpc_server* server, const grpc_channel_args* args, void* /*reserved*/) {
  // Match inproc: ensure callback/exec contexts exist while we build channel.
  grpc_core::ExecCtx exec_ctx;
  // Use the same channel-arg preconditioning as inproc before creating channel.
  auto client_args = grpc_core::CoreConfiguration::Get()
                         .channel_args_preconditioning()
                         .PreconditionChannelArgs(args);
  
  // Follow the same pattern as inproc: check if we should use promise-based transport
  // For now, always use legacy transport to match inproc behavior (IsPromiseBasedInprocTransportEnabled() returns false)
  bool use_promise_based = client_args
      .GetBool("grpc.experimental.promise_based_shmem_transport")
      .value_or(false);  // Default to false like inproc
      
  if (!use_promise_based) {
    return grpc_legacy_shmem_channel_create(server, args, nullptr);
  }
  
  auto ch = grpc_core::MakeShmemChannel(grpc_core::Server::FromC(server),
                                        std::move(client_args));
  return ch.release()->c_ptr();
}
