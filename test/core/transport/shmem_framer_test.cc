// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "src/core/ext/transport/shmem/shmem_framer.h"
#include "src/core/ext/transport/shmem/shmem_protocol.h"

namespace grpc_shmem {

TEST(ShmemFramerTest, SerializeDeserializeMetadataKVs) {
  std::vector<KVPair> in = {
      {":path", "/service/method"},
      {"user-agent", "grpc-c/1.0"},
      {"x-test", "abc123"},
  };
  auto bytes = SerializeMetadataKVs(in);
  auto out = DeserializeMetadataKVs(bytes.data(), bytes.size());
  ASSERT_EQ(out.size(), in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    EXPECT_EQ(out[i].key, in[i].key);
    EXPECT_EQ(out[i].value, in[i].value);
  }
}

}  // namespace grpc_shmem

// Provide a simple gtest main for standalone execution
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
