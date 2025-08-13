// Temporary stub implementation to satisfy build targets while we redesign.
#include "src/core/ext/transport/shmem/shmem_transport.h"

#include <utility>

namespace grpc_core {

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& /*server_channel_args*/) {
  // For now return {nullptr, nullptr} to keep tests from attempting to run
  // until the transport is implemented. Tests that depend on this will need to
  // be guarded to skip if nullptrs are returned.
  return {OrphanablePtr<Transport>(nullptr), OrphanablePtr<Transport>(nullptr)};
}

}  // namespace grpc_core
