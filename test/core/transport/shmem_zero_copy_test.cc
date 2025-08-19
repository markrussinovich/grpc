// Copyright 2025 gRPC authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>

#include <cstring>

#include "include/grpc/slice.h"
#include "src/core/ext/transport/shmem/shmem_queue.h"
#include "src/core/ext/transport/shmem/shmem_framer.h"

namespace grpc_shmem {

TEST(ShmemZeroCopyTest, SliceAdvancesTailOnDestroy) {
  // Arrange a small ring with static backing storage
  DataRingBuffer rb;
  constexpr size_t cap = 256;
  static unsigned char storage[cap];
  rb.capacity = cap;
  rb.head.store(0);
  rb.tail.store(0);
  // Use storage as fake segment base, so buffer_offset = 0 points to storage
  void* fake_segment_base = storage;
  rb.buffer_offset = 0;  // Offset 0 from fake_segment_base points to storage

  // Reserve and write some payload
  const char* msg = "hello world";
  const uint32_t sz = static_cast<uint32_t>(strlen(msg));
  uint64_t off = 0;
  ASSERT_TRUE(ReserveContiguous(&rb, sz, &off));
  std::memcpy(rb.GetBuffer(fake_segment_base) + off, msg, sz);

  // Create zero-copy slice and ensure contents match
  grpc_slice s = MakeSliceFromRing(&rb, fake_segment_base, off, sz);
  ASSERT_EQ(rb.tail.load(), 0u);
  EXPECT_EQ(GRPC_SLICE_LENGTH(s), sz);
  EXPECT_EQ(std::memcmp(GRPC_SLICE_START_PTR(s), msg, sz), 0);

  // Destroy slice -> should advance tail by sz
  grpc_slice_unref(s);
  EXPECT_EQ(rb.tail.load(), sz);
}

}  // namespace grpc_shmem

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
