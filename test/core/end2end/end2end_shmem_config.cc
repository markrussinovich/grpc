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

#include <grpc/grpc.h>

#include <memory>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "src/core/lib/channel/channel_args.h"
#include "test/core/end2end/end2end_tests.h"

// shmem channel factory
extern "C" grpc_channel* grpc_shmem_channel_create(
    grpc_server* server, const grpc_channel_args* args, void* reserved);

namespace grpc_core {

class ShmemFixture : public grpc_core::CoreTestFixture {
 public:
  ShmemFixture() = default;

 private:
  grpc_server* MakeServer(
      const grpc_core::ChannelArgs& args, grpc_completion_queue* cq,
      absl::AnyInvocable<void(grpc_server*)>& pre_server_start) override {
    if (made_server_ != nullptr) return made_server_;
    made_server_ = grpc_server_create(args.ToC().get(), nullptr);
    grpc_server_register_completion_queue(made_server_, cq, nullptr);
    pre_server_start(made_server_);
    grpc_server_start(made_server_);
    return made_server_;
  }
  
  grpc_channel* MakeClient(const grpc_core::ChannelArgs& args,
                           grpc_completion_queue* cq) override {
    absl::AnyInvocable<void(grpc_server*)>
        pre_server_start = [](grpc_server*) {};
    grpc_server* server = MakeServer(args, cq, pre_server_start);
    return grpc_shmem_channel_create(server, args.ToC().get(), nullptr);
  }

  grpc_server* made_server_ = nullptr;
};

std::vector<CoreTestConfiguration> End2endTestConfigs() {
  return std::vector<CoreTestConfiguration>{
      CoreTestConfiguration{
          "Shmem",
          FEATURE_MASK_DOES_NOT_SUPPORT_WRITE_BUFFERING,
          nullptr,
          [](const ChannelArgs&, const ChannelArgs&) {
            return std::make_unique<ShmemFixture>();
          },
      },
  };
}

}  // namespace grpc_core