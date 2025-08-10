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

#include "src/core/ext/transport/shmem/shmem_transport.h"

#include <atomic>
#include <memory>

#include "absl/status/status.h"
#include "src/core/lib/transport/transport.h"

namespace grpc_core {
namespace {

class ShmemServerTransport;

class ShmemClientTransport final : public ClientTransport {
 public:
  explicit ShmemClientTransport() {}

  void StartCall(CallHandler /*child_call_handler*/) override {
    // TODO(shmem): implement
  }
  void Orphan() override { Unref(); }
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return this; }
  ServerTransport* server_transport() override { return nullptr; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op*) override {}

 private:
  ~ShmemClientTransport() override = default;
};

class ShmemServerTransport final : public ServerTransport {
 public:
  explicit ShmemServerTransport(const ChannelArgs& /*args*/) {}

  void SetCallDestination(
      RefCountedPtr<UnstartedCallDestination> /*unstarted_call_handler*/)
      override {}

  void Orphan() override { Unref(); }
  FilterStackTransport* filter_stack_transport() override { return nullptr; }
  ClientTransport* client_transport() override { return nullptr; }
  ServerTransport* server_transport() override { return this; }
  absl::string_view GetTransportName() const override { return "shmem"; }
  RefCountedPtr<channelz::SocketNode> GetSocketNode() const override {
    return nullptr;
  }
  void SetPollset(grpc_stream*, grpc_pollset*) override {}
  void SetPollsetSet(grpc_stream*, grpc_pollset_set*) override {}
  void PerformOp(grpc_transport_op* op) override {
    ExecCtx::Run(DEBUG_LOCATION, op->on_consumed, absl::OkStatus());
  }

 private:
  ~ShmemServerTransport() override = default;
};

}  // namespace

std::pair<OrphanablePtr<Transport>, OrphanablePtr<Transport>>
MakeShmemTransportPair(const ChannelArgs& server_channel_args) {
  auto server_transport =
      MakeOrphanable<ShmemServerTransport>(server_channel_args);
  auto client_transport = MakeOrphanable<ShmemClientTransport>();
  return std::pair(std::move(client_transport), std::move(server_transport));
}

}  // namespace grpc_core
