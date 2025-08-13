// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>

#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

TEST(ShmemStructsTest, CommandQueuePushPop) {
  // CommandQueue is an SPSC queue; exercise simple push/pop on a local instance.
  CommandQueue q;
  Command in{};
  in.stream_id = 42;
  in.type = FrameType::C2S_MESSAGE;
  in.data_offset = 128;
  in.data_size = 256;
  in.grpc_status_code = 0;
  in.inline_data = 0;

  // Queue should accept a push and then return it via pop.
  ASSERT_TRUE(q.push(in));
  Command out;
  ASSERT_TRUE(q.pop(out));
  EXPECT_EQ(out.stream_id, 42u);
  EXPECT_EQ(static_cast<int>(out.type), static_cast<int>(FrameType::C2S_MESSAGE));
  EXPECT_EQ(out.data_offset, 128u);
  EXPECT_EQ(out.data_size, 256u);
}

}  // namespace grpc_shmem

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
