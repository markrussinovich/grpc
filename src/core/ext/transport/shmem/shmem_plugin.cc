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

#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/transport/endpoint_transport.h"
#include "src/core/server/server.h"
#include "src/core/lib/surface/channel_create.h"
#include "src/core/util/uri.h"
#include "src/core/client_channel/direct_channel.h"
#include "src/core/ext/transport/shmem/shmem_segment.h"
#include "src/core/transport/auth_context.h"
#include "src/core/call/security_context.h"
#include "absl/strings/str_cat.h"
#include "absl/container/flat_hash_map.h"
#include <mutex>

// Forward declaration of shmem channel creation function
extern "C" grpc_channel* grpc_shmem_channel_create(
    grpc_server* server, const grpc_channel_args* args, void* reserved);

namespace grpc_core {
namespace {

// Cross-process registry for shmem servers using named shared memory segments
class ShmemServerRegistry {
public:
  static ShmemServerRegistry& Get() {
    static ShmemServerRegistry instance;
    return instance;
  }
  
  // Register a server - don't create transport yet, wait for first client
  void RegisterServer(const std::string& name, Server* server) {
    std::lock_guard<std::mutex> lock(mutex_);
    servers_[name] = server;
  }
  
  void UnregisterServer(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    servers_.erase(name);
    // Segment cleanup happens automatically when server transport is destroyed
    grpc_shmem::ShmemSegment::RemoveIfExists(absl::StrCat("grpc_shmem_", name));
    grpc_shmem::ShmemSegment::RemoveNamedSemaphores(name);
  }
  
  bool HasServer(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return servers_.find(name) != servers_.end();
  }
  
private:
  std::mutex mutex_;
  absl::flat_hash_map<std::string, Server*> servers_;
};

class ShmemEndpointTransport final : public EndpointTransport {
 public:
  absl::StatusOr<grpc_channel*> ChannelCreate(
      std::string target, const ChannelArgs& args) override {
    VLOG(1) << "ChannelCreate called with target: " << target;
    // Parse shmem:// URI to extract server name
    auto uri_result = URI::Parse(target);
    if (!uri_result.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid shmem URI: ", target));
    }
    auto uri = uri_result.value();
    if (uri.scheme() != "shmem") {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected shmem scheme, got: ", uri.scheme()));
    }
    if (uri.authority().empty()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Shmem URI missing server name: ", target));
    }

    std::string server_name = uri.authority();
    
    // Try to connect to the named shared memory segment
    // Force ring mode (not dispatch-only) for cross-process communication
    ChannelArgs client_args = args
        .Set("grpc.experimental.promise_based_shmem_transport", true)
        .Set("grpc.shmem.dispatch_only", false)
        .Set("grpc.shmem.server_name", server_name);
    
    auto client_transport = ConnectToShmemServerTransport(server_name, client_args);
    if (!client_transport) {
      return absl::NotFoundError(
          absl::StrCat("Failed to connect to shmem server: ", server_name));
    }
    
    // Create direct channel with the client transport
    ChannelArgs args_with_transport = client_args
        .SetObject(client_transport.get())
        .Set(GRPC_ARG_DEFAULT_AUTHORITY, server_name);  // Set default authority
    client_transport.release(); // Channel takes ownership
    
    auto channel = DirectChannel::Create("shmem://" + server_name, args_with_transport);
    if (!channel.ok()) {
      return channel.status();
    }
    
    return channel->release()->c_ptr();
  }

  absl::StatusOr<int> AddPort(Server* server, std::string addr,
                              const ChannelArgs& args) override {
    // Parse shmem:// URI to extract server name
    auto uri_result = URI::Parse(addr);
    if (!uri_result.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid shmem URI: ", addr));
    }
    auto uri = uri_result.value();
    if (uri.scheme() != "shmem") {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected shmem scheme, got: ", uri.scheme()));
    }
    if (uri.authority().empty()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Shmem URI missing server name: ", addr));
    }

    // Register server and create transport (but reader thread won't start until SetCallDestination)
    std::string server_name = uri.authority();
    VLOG(1) << "AddPort called for shmem server: " << server_name;
    LOG(INFO) << "AddPort: Registering shmem server: " << server_name;
    VLOG(2) << "AddPort: Registering shmem server: " << server_name;
    ShmemServerRegistry::Get().RegisterServer(server_name, server);
    
    // Create properly configured auth context for both transport and server setup  
    VLOG(2) << "Plugin - Creating properly configured auth context";
    
    // Create auth context like MakeShmemAuthContext() does
    auto auth_context = grpc_core::MakeRefCounted<grpc_auth_context>(nullptr);
    grpc_auth_context_add_cstring_property(
        auth_context.get(),
        GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME,
        "shmem");
    grpc_auth_context_set_peer_identity_property_name(
        auth_context.get(),
        GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME);
        
    VLOG(2) << "Plugin - Created configured auth context: " << auth_context.get();
    
    // Add auth context to args for transport creation
    ChannelArgs transport_args = args.SetObject(auth_context);
    
    // Create the named server transport with auth context
    VLOG(2) << "AddPort: Creating server transport...";
    auto server_transport = grpc_core::MakeNamedShmemServerTransport(
        server_name, transport_args);
    if (!server_transport) {
      LOG(ERROR) << "AddPort: Failed to create server transport";
      return absl::InternalError("Failed to create shmem server transport");
    }
    
    VLOG(2) << "AddPort: Setting up transport with server...";
    
    VLOG(2) << "Plugin - Using same auth context for SetupTransport: " << auth_context.get();
    
    // Use similar channel args to inproc transport with the same auth context
    ChannelArgs setup_args = args
        .Remove(GRPC_ARG_MAX_CONNECTION_IDLE_MS)
        .Remove(GRPC_ARG_MAX_CONNECTION_AGE_MS)
        .SetObject(auth_context);
        
    LOG(INFO) << "Plugin - About to call SetupTransport with auth context: " << auth_context.get();
    VLOG(2) << "Plugin - About to call SetupTransport with auth context: " << auth_context.get();
    
    auto result = server->SetupTransport(server_transport.get(), nullptr, setup_args, nullptr);
    if (!result.ok()) {
      LOG(ERROR) << "AddPort: Failed to setup transport: " << result;
      return result;
    }
    
    server_transport.release(); // Server takes ownership
    LOG(INFO) << "AddPort: Server transport set up successfully";
    VLOG(2) << "AddPort: Server transport set up successfully";
    
    // Return a fake port number since shmem doesn't use real network ports
    return 1;
  }
};

}  // namespace

void RegisterShmemTransport(CoreConfiguration::Builder* builder) {
  VLOG(1) << "RegisterShmemTransport called";
  LOG(INFO) << "RegisterShmemTransport called - registering shmem endpoint transport";
  builder->endpoint_transport_registry()->RegisterTransport(
      "shmem", std::make_unique<ShmemEndpointTransport>());
  LOG(INFO) << "RegisterShmemTransport completed - shmem transport registered";
  VLOG(1) << "RegisterShmemTransport completed";
}

}  // namespace grpc_core