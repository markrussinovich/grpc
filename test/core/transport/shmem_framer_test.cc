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
#include "src/core/ext/transport/shmem/shmem_segment.h"

namespace grpc_shmem {

TEST(ShmemFramerTest, SerializeParseHeaderRoundTrip) {
  FrameHeader hdr{};
  hdr.frame_size = static_cast<uint32_t>(kSerializedHeaderSize + 7);
  hdr.stream_id = 42;
  hdr.type = FrameType::C2S_MESSAGE;
  hdr.flags = FrameFlags::MORE_FRAMES_FOLLOW;
  hdr.reserved = 0xBEEF;

  uint8_t buf[kSerializedHeaderSize];
  SerializeHeaderLE(hdr, buf);
  FrameHeader parsed = ParseHeaderLE(buf);

  EXPECT_EQ(parsed.frame_size, hdr.frame_size);
  EXPECT_EQ(parsed.stream_id, hdr.stream_id);
  EXPECT_EQ(static_cast<uint8_t>(parsed.type), static_cast<uint8_t>(hdr.type));
  EXPECT_EQ(static_cast<uint8_t>(parsed.flags),
            static_cast<uint8_t>(hdr.flags));
  EXPECT_EQ(parsed.reserved, hdr.reserved);
}

TEST(ShmemFramerTest, WriteReadFrameC2SAndS2C) {
  // Create a small shared memory segment for the test
  SegmentConfig cfg;
  cfg.name = "grpc_shmem_framer_test";
  cfg.size = 1 * 1024 * 1024;         // 1 MiB total
  cfg.queue_capacity = 64 * 1024;     // 64 KiB per-queue
  ShmemSegment::RemoveIfExists(cfg.name);
  auto seg = ShmemSegment::Create(cfg);
  ControlBlock* cb = seg.control();

  // Prepare a payload
  const std::string payload = "hello shmem";
  const uint32_t frame_size =
      static_cast<uint32_t>(kSerializedHeaderSize + payload.size());

  // Write to C2S in a thread and read back in main thread
  std::thread writer([&] {
    FrameHeader w_hdr{};
    w_hdr.frame_size = frame_size;
    w_hdr.stream_id = 1;
    w_hdr.type = FrameType::C2S_INITIAL_METADATA;
    w_hdr.flags = FrameFlags::NONE;
    w_hdr.reserved = 0;
    WriteFrame(cb, QueueKind::kC2S, w_hdr,
               reinterpret_cast<const uint8_t*>(payload.data()),
               payload.size());
  });

  FrameHeader r_hdr{};
  std::vector<uint8_t> got = ReadFrame(cb, QueueKind::kC2S, &r_hdr);
  writer.join();

  EXPECT_EQ(r_hdr.frame_size, frame_size);
  EXPECT_EQ(r_hdr.stream_id, 1u);
  EXPECT_EQ(static_cast<uint8_t>(r_hdr.type),
            static_cast<uint8_t>(FrameType::C2S_INITIAL_METADATA));
  EXPECT_EQ(got.size(), payload.size());
  EXPECT_EQ(std::string(reinterpret_cast<char*>(got.data()), got.size()),
            payload);

  // Repeat for S2C
  std::thread writer2([&] {
    FrameHeader w_hdr{};
    w_hdr.frame_size = frame_size;
    w_hdr.stream_id = 2;
    w_hdr.type = FrameType::S2C_MESSAGE;
    w_hdr.flags = FrameFlags::NONE;
    w_hdr.reserved = 0;
    WriteFrame(cb, QueueKind::kS2C, w_hdr,
               reinterpret_cast<const uint8_t*>(payload.data()),
               payload.size());
  });

  FrameHeader r_hdr2{};
  std::vector<uint8_t> got2 = ReadFrame(cb, QueueKind::kS2C, &r_hdr2);
  writer2.join();

  EXPECT_EQ(r_hdr2.frame_size, frame_size);
  EXPECT_EQ(r_hdr2.stream_id, 2u);
  EXPECT_EQ(static_cast<uint8_t>(r_hdr2.type),
            static_cast<uint8_t>(FrameType::S2C_MESSAGE));
  EXPECT_EQ(got2.size(), payload.size());
  EXPECT_EQ(std::string(reinterpret_cast<char*>(got2.data()), got2.size()),
            payload);
}

}  // namespace grpc_shmem

// Provide a simple gtest main for standalone execution
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
