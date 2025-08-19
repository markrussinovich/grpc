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

#include "src/core/ext/transport/shmem/shmem_resolver.h"

#include <memory>
#include <string>
#include <sys/un.h>

#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/core/config/core_configuration.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/resolver/endpoint_addresses.h"
#include "src/core/resolver/resolver.h"
#include "src/core/resolver/resolver_factory.h"
#include "src/core/resolver/resolver_registry.h"
#include "src/core/util/orphanable.h"
#include "src/core/util/uri.h"

namespace grpc_core {
namespace {

// Shmem resolver that creates a result with special channel args
// to signal the use of shmem transport
class ShmemResolver final : public Resolver {
 public:
  ShmemResolver(std::string server_name, ResolverArgs args);

  void StartLocked() override;
  void ShutdownLocked() override {}

 private:
  std::unique_ptr<ResultHandler> result_handler_;
  std::string server_name_;
  ChannelArgs channel_args_;
};

ShmemResolver::ShmemResolver(std::string server_name, ResolverArgs args)
    : result_handler_(std::move(args.result_handler)),
      server_name_(std::move(server_name)),
      channel_args_(std::move(args.args)) {}

void ShmemResolver::StartLocked() {
  Result result;
  
  // Create a special endpoint address that indicates shmem transport
  // We use a fake address since shmem doesn't use real network addresses
  grpc_resolved_address addr;
  memset(&addr, 0, sizeof(addr));
  addr.len = sizeof(struct sockaddr_un);
  
  // Create endpoint addresses with shmem-specific attributes
  EndpointAddresses endpoint(
      addr, 
      ChannelArgs().Set("grpc.shmem.server_name", server_name_));
  
  result.addresses = EndpointAddressesList({std::move(endpoint)});
  
  // Add channel args to indicate shmem transport should be used
  result.args = channel_args_
      .Set("grpc.internal.use_shmem_transport", true)
      .Set("grpc.shmem.server_name", server_name_);
      
  result_handler_->ReportResult(std::move(result));
}

// Shmem resolver factory
class ShmemResolverFactory final : public ResolverFactory {
 public:
  absl::string_view scheme() const override { return "shmem"; }

  bool IsValidUri(const URI& uri) const override {
    // Accept shmem://server-name format
    // Path should be empty or just "/" and authority should contain server name
    if (uri.authority().empty()) {
      return false;
    }
    return true;
  }

  OrphanablePtr<Resolver> CreateResolver(ResolverArgs args) const override {
    std::string server_name = args.uri.authority();
    if (server_name.empty()) {
      LOG(ERROR) << "Shmem URI missing server name: " << args.uri.ToString();
      return nullptr;
    }
    
    return MakeOrphanable<ShmemResolver>(std::move(server_name), std::move(args));
  }

  std::string GetDefaultAuthority(const URI& uri) const override {
    return uri.authority();
  }
};

}  // namespace

void RegisterShmemResolver(CoreConfiguration::Builder* builder) {
  builder->resolver_registry()->RegisterResolverFactory(
      std::make_unique<ShmemResolverFactory>());
}

}  // namespace grpc_core