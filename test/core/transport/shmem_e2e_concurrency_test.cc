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
#include <string>
#include <vector>

#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/call/call_spine.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/resource_quota/memory_quota.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/lib/promise/seq.h"
#include "src/core/util/notification.h"
#include "include/grpc/event_engine/event_engine.h"

namespace grpc_core {

// Heavier concurrency validation: multiple concurrent streams with interleaved
// small and large payloads. Ensures demuxed reader routes frames correctly.
TEST(ShmemE2EConcurrency, ManyConcurrentStreamsInterleaved) {
  ExecCtx exec_ctx;

  ChannelArgs args = CoreConfiguration::Get()
                         .channel_args_preconditioning()
                         .PreconditionChannelArgs(nullptr);

  auto pair = MakeShmemTransportPair(args);
  auto client = std::move(pair.first);
  auto server = std::move(pair.second);

  class ServerCallDestination : public UnstartedCallDestination {
   public:
    void StartCall(UnstartedCallHandler handler) override { (void)handler.StartCall(); }
    void Orphaned() override {}
  } dest;
  server->server_transport()->SetCallDestination(MakeRefCounted<ServerCallDestination>());

  auto rq = MakeResourceQuota("shmem-e2e-concurrency");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("shmem-e2e-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();

  const int kN = 8;  // keep moderate to be fast and deterministic
  std::vector<CallInitiatorAndHandler> calls;
  calls.reserve(kN);
  std::vector<std::string> expect(kN);
  std::vector<Notification> imd(kN), got(kN), tr(kN);
  std::vector<std::string> echoed(kN);
  echoed.resize(kN);

  for (int i = 0; i < kN; ++i) {
    auto arena = call_arena_allocator->MakeArena();
    arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
    auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
    md->Set(HttpPathMetadata(), Slice::FromExternalString("/c/" + std::to_string(i)));
    calls.push_back(MakeCallPair(std::move(md), std::move(arena)));
  }

  // Start all calls
  for (int i = 0; i < kN; ++i) {
    calls[i].handler.SpawnInfallible("start", [c = client.get(), h = calls[i].handler]() mutable {
      c->client_transport()->StartCall(h.StartCall());
      return Empty{};
    });
  }

  // Build payloads: even indices small, odd indices ~1MiB large
  for (int i = 0; i < kN; ++i) {
    if ((i % 2) == 0) {
      expect[i] = "small-" + std::to_string(i);
    } else {
      const size_t sz = 1 * 1024 * 1024;  // 1 MiB
      std::string p;
      p.resize(sz);
      for (size_t j = 0; j < sz; ++j) p[j] = static_cast<char>('a' + ((i + j) % 26));
      expect[i] = std::move(p);
    }
  }

  // Send all payloads; interleave to exercise demux
  for (int i = 0; i < kN; ++i) {
    auto payload = expect[i];
    calls[i].initiator.SpawnInfallible("send", [in = calls[i].initiator, payload]() mutable {
      return Seq(in.PushMessage(Arena::MakePooled<Message>(SliceBuffer(Slice::FromCopiedString(payload)), 0)),
                 [in](StatusFlag) mutable {
                   in.FinishSends();
                   return Empty{};
                 });
    });
  }

  // Await initial metadata for all
  for (int i = 0; i < kN; ++i) {
    calls[i].initiator.SpawnInfallible("imd", [in = calls[i].initiator, &note = imd[i]]() mutable {
      return Seq(in.PullServerInitialMetadata(), [&note](auto) { note.Notify(); return Empty{}; });
    });
  }
  for (int t = 0; t < 40000; ++t) {
    bool all = true;
    for (int i = 0; i < kN; ++i) all &= imd[i].HasBeenNotified();
    if (all) break;
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (int i = 0; i < kN; ++i) ASSERT_TRUE(imd[i].HasBeenNotified());

  // Await one echoed message per call
  for (int i = 0; i < kN; ++i) {
    calls[i].initiator.SpawnInfallible("msg", [in = calls[i].initiator, &dst = echoed[i], &note = got[i]]() mutable {
      return Seq(in.PullMessage(), [&dst, &note](ServerToClientNextMessage m) {
        if (m.ok() && m.has_value()) { dst = m.value().payload()->JoinIntoString(); note.Notify(); }
        return Empty{};
      });
    });
  }
  for (int t = 0; t < 180000; ++t) {  // allow extra for large frames
    bool all = true;
    for (int i = 0; i < kN; ++i) all &= got[i].HasBeenNotified();
    if (all) break;
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (int i = 0; i < kN; ++i) {
    ASSERT_TRUE(got[i].HasBeenNotified());
    ASSERT_EQ(echoed[i].size(), expect[i].size());
    // Spot-check ends for large payloads, full-eq is fine for small
    if (expect[i].size() <= 64) {
      EXPECT_EQ(echoed[i], expect[i]);
    } else {
      EXPECT_EQ(echoed[i].substr(0, 64), expect[i].substr(0, 64));
      EXPECT_EQ(echoed[i].substr(echoed[i].size() - 64), expect[i].substr(expect[i].size() - 64));
    }
  }

  // Await trailers for all
  for (int i = 0; i < kN; ++i) {
    calls[i].initiator.SpawnInfallible("tr", [in = calls[i].initiator, &note = tr[i]]() mutable {
      return Seq(in.PullServerTrailingMetadata(), [&note](ServerMetadataHandle) { note.Notify(); return Empty{}; });
    });
  }
  for (int t = 0; t < 60000; ++t) {
    bool all = true;
    for (int i = 0; i < kN; ++i) all &= tr[i].HasBeenNotified();
    if (all) break;
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (int i = 0; i < kN; ++i) ASSERT_TRUE(tr[i].HasBeenNotified());

  client.reset();
  server.reset();
}

}  // namespace grpc_core

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  grpc_init();
  int r = RUN_ALL_TESTS();
  grpc_shutdown();
  return r;
}
