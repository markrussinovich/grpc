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

#include "src/core/ext/transport/shmem/shmem_memory.h"

#include <gtest/gtest.h>

namespace grpc_shmem {

TEST(ShmemMemoryTest, ControlBlockDefaults) {
  ControlBlock cb;
  EXPECT_EQ(cb.transport_version, 1u);
  EXPECT_EQ(cb.server_state.load(), 0u);
  EXPECT_EQ(cb.client_state.load(), 0u);
  EXPECT_EQ(cb.c2s_queue.get(), nullptr);
  EXPECT_EQ(cb.s2c_queue.get(), nullptr);
}

TEST(ShmemMemoryTest, RingBufferPointersAdvance) {
  RingBuffer rb;
  rb.capacity = 1024;
  EXPECT_EQ(rb.head.load(), 0u);
  EXPECT_EQ(rb.tail.load(), 0u);
  // Simulate a write of 100 bytes
  rb.head.store(100);
  EXPECT_EQ(rb.head.load(), 100u);
  // Simulate a read of 60 bytes
  rb.tail.store(60);
  EXPECT_EQ(rb.tail.load(), 60u);
}

}  // namespace grpc_shmem

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
