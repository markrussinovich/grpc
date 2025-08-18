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

#endif
