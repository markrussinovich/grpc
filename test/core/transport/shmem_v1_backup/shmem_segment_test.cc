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

#include "src/core/ext/transport/shmem/shmem_segment.h"

#include <gtest/gtest.h>

namespace grpc_shmem {

TEST(ShmemSegmentTest, CreateOpenAndVerify) {
  const std::string name = "grpc_shmem_test_segment";
  ShmemSegment::RemoveIfExists(name);

  SegmentConfig cfg{.name = name, .size = 4 * 1024 * 1024, .queue_capacity = 64 * 1024};
  auto server = ShmemSegment::Create(cfg);
  auto* cb = server.control();
  ASSERT_NE(cb, nullptr);
  EXPECT_EQ(cb->magic_number, kMagic);
  EXPECT_EQ(cb->transport_version, kVersion);
  EXPECT_EQ(cb->server_state.load(), 1u);
  EXPECT_EQ(cb->client_state.load(), 0u);
  ASSERT_NE(cb->c2s_queue.get(), nullptr);
  ASSERT_NE(cb->s2c_queue.get(), nullptr);
  EXPECT_EQ(cb->c2s_queue->capacity, cfg.queue_capacity);
  EXPECT_EQ(cb->s2c_queue->capacity, cfg.queue_capacity);
  ASSERT_NE(cb->c2s_queue->buffer.get(), nullptr);
  ASSERT_NE(cb->s2c_queue->buffer.get(), nullptr);

  // Open client view
  auto client = ShmemSegment::Open(name);
  EXPECT_EQ(client.control()->client_state.load(), 1u);

  // Cleanup
  ShmemSegment::RemoveIfExists(name);
}

}  // namespace grpc_shmem

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
