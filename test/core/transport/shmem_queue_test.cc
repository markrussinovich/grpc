// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>

#include <thread>
#include <chrono>

#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"

using namespace std::chrono_literals;

namespace grpc_shmem {

TEST(ShmemQueueTest, ReserveAndPushPopWithSem) {
  // Build a local ControlBlock (stack) and queues not in shared memory to test logic.
  ControlBlock cb;  // semaphores start at 0
  ShmemQueues queues;
  // Allocate a small local buffer for the ring
  constexpr size_t cap = 1024;
  static unsigned char storage[cap];
  queues.data_rb.capacity = cap;
  queues.data_rb.head.store(0);
  queues.data_rb.tail.store(0);
  queues.data_rb.buffer = storage;

  // Start a consumer thread that waits for one command
  Command got;
  std::thread t([&] {
    ASSERT_TRUE(PopCommandHybrid(&queues, &cb, Direction::kC2S, /*spin_iters=*/1000, &got));
  });

  // Producer reserves and writes a payload, then pushes a command
  uint64_t off = 0;
  ASSERT_TRUE(ReserveContiguous(&queues.data_rb, 64, &off));
  // Simulate a write
  for (int i = 0; i < 64; ++i) queues.data_rb.buffer.get()[off + i] = static_cast<unsigned char>(i);
  Command c{};
  c.stream_id = 1;
  c.type = FrameType::C2S_MESSAGE;
  c.data_offset = off;
  c.data_size = 64;
  ASSERT_TRUE(PushCommand(&queues, &cb, Direction::kC2S, c));

  t.join();

  // Verify consumer saw the command
  EXPECT_EQ(got.stream_id, 1u);
  EXPECT_EQ(static_cast<int>(got.type), static_cast<int>(FrameType::C2S_MESSAGE));
  EXPECT_EQ(got.data_offset, off);
  EXPECT_EQ(got.data_size, 64u);

  // Consumer releases tail
  Release(&queues.data_rb, got.data_size);
  EXPECT_EQ(queues.data_rb.tail.load(), 64u);
}

}  // namespace grpc_shmem

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
