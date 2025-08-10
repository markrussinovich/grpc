// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "src/core/ext/transport/shmem/shmem_segment.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"

namespace grpc_shmem {

TEST(ShmemQueueTest, BlockingReadWriteWraps) {
  const std::string name = "grpc_shmem_qtest";
  ShmemSegment::RemoveIfExists(name);
  SegmentConfig cfg{.name = name, .size = 2 * 1024 * 1024, .queue_capacity = 1024};
  auto server = ShmemSegment::Create(cfg);
  auto client = ShmemSegment::Open(name);

  // Prepare payload larger than capacity to exercise wrap-around in chunks.
  std::vector<uint8_t> payload(4096);
  for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i & 0xFF);
  std::vector<uint8_t> received(payload.size());

  std::thread prod([&](){ RingWriteBlocking(server.control(), QueueKind::kC2S, payload.data(), payload.size()); });
  std::thread cons([&](){ RingReadBlocking(client.control(), QueueKind::kC2S, received.data(), received.size()); });

  prod.join();
  cons.join();

  EXPECT_EQ(payload, received);
  ShmemSegment::RemoveIfExists(name);
}

}  // namespace grpc_shmem
