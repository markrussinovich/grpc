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
        .Set("grpc.shmem.dispatch_only", false);
    
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
    std::cout << "AddPort: Registering shmem server: " << server_name << std::endl;
    ShmemServerRegistry::Get().RegisterServer(server_name, server);
    
    // Create the named server transport 
    std::cout << "AddPort: Creating server transport..." << std::endl;
    auto server_transport = grpc_core::MakeNamedShmemServerTransport(
        server_name, args);
    if (!server_transport) {
      std::cout << "AddPort: Failed to create server transport" << std::endl;
      return absl::InternalError("Failed to create shmem server transport");
    }
    
    std::cout << "AddPort: Setting up transport with server..." << std::endl;
    // Create insecure auth context like insecure_security_connector does
    auto auth_context = grpc_core::MakeRefCounted<grpc_auth_context>(nullptr);
    // Use similar channel args to inproc transport and add auth context
    ChannelArgs setup_args = args
        .Remove(GRPC_ARG_MAX_CONNECTION_IDLE_MS)
        .Remove(GRPC_ARG_MAX_CONNECTION_AGE_MS)
        .SetObject(auth_context);
    auto result = server->SetupTransport(server_transport.get(), nullptr, setup_args, nullptr);
    if (!result.ok()) {
      std::cout << "AddPort: Failed to setup transport: " << result << std::endl;
      return result;
    }
    
    server_transport.release(); // Server takes ownership
    std::cout << "AddPort: Server transport set up successfully" << std::endl;
    
    // Return a fake port number since shmem doesn't use real network ports
    return 1;
  }
};

}  // namespace

void RegisterShmemTransport(CoreConfiguration::Builder* builder) {
  builder->endpoint_transport_registry()->RegisterTransport(
      "shmem", std::make_unique<ShmemEndpointTransport>());
}

}  // namespace grpc_core