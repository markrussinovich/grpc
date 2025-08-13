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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H

#include <cstdint>

namespace grpc_shmem {

enum class FrameType : uint8_t {
  // Client-to-Server Frame Types
  C2S_INITIAL_METADATA = 0x01,
  C2S_MESSAGE = 0x02,
  C2S_TRAILING_METADATA = 0x03,  // Sent by client for client-streaming or bidi
  C2S_CANCEL = 0x04,

  // Server-to-Client Frame Types
  S2C_INITIAL_METADATA = 0x81,
  S2C_MESSAGE = 0x82,
  S2C_TRAILING_METADATA = 0x83,  // Contains final status
};

enum class FrameFlags : uint8_t {
  NONE = 0x00,
  // Indicates this is not the final frame for a given logical message.
  MORE_FRAMES_FOLLOW = 0x01,
};

struct FrameHeader {
  // Total size of the frame in bytes, including this header and the payload.
  uint32_t frame_size;
  // The gRPC stream identifier this frame belongs to.
  uint32_t stream_id;
  // The type of the frame, from the FrameType enum.
  FrameType type;
  // Flags for the frame, from the FrameFlags enum.
  FrameFlags flags;
  // Reserved for alignment and future use.
  uint16_t reserved;
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_PROTOCOL_H
