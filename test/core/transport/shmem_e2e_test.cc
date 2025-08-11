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

#include "absl/strings/string_view.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/call/call_spine.h"
#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "include/grpc/event_engine/event_engine.h"
#include "src/core/lib/resource_quota/memory_quota.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/lib/transport/transport.h"
#include "src/core/lib/promise/seq.h"
#include "src/core/util/notification.h"

namespace grpc_core {

// Minimal E2E test that uses the shmem transport directly without the yodel
// test suite (to avoid pulling in fuzztest).
TEST(ShmemE2E, MetadataOnlyUnaryReturnsUnimplemented) {
  ExecCtx exec_ctx;

  // Build channel args with a ResourceQuota and EventEngine.
  ChannelArgs args = CoreConfiguration::Get()
                         .channel_args_preconditioning()
                         .PreconditionChannelArgs(nullptr);

  // Create a transport pair.
  auto pair = MakeShmemTransportPair(args);
  auto client = std::move(pair.first);
  auto server = std::move(pair.second);

  // Set a simple server call destination to satisfy the server transport.
  class ServerCallDestination : public UnstartedCallDestination {
   public:
    void StartCall(UnstartedCallHandler handler) override {
      // For this minimal E2E, we don't need to process the server-side call.
      // Just start it so the pipeline is consistent.
      (void)handler.StartCall();
    }
    void Orphaned() override {}

   private:
  } dest;

  server->server_transport()->SetCallDestination(MakeRefCounted<ServerCallDestination>());

  // Create a call with a path.
  auto rq = MakeResourceQuota("shmem-e2e");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("shmem-e2e-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  auto arena = call_arena_allocator->MakeArena();
  // Set the EventEngine context on the arena explicitly.
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

  auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
  md->Set(HttpPathMetadata(), Slice::FromExternalString("/foo/bar"));

  auto call = MakeCallPair(std::move(md), std::move(arena));

  // Start the client call on the transport.
  call.handler.SpawnInfallible(
      "start-call", [c = client.get(), h = call.handler]() mutable {
        c->client_transport()->StartCall(h.StartCall());
        return Empty{};
      });
  // Indicate no payloads will be sent (half-close) to help completion.
  call.initiator.SpawnFinishSends();

  // Client waits for initial metadata and then trailing metadata.
  // Await server initial metadata inside the party and notify this thread.
  std::optional<ServerMetadataHandle> got_initial;
  Notification initial_ready;
  call.initiator.SpawnInfallible(
      "await-initial",
      [i = call.initiator, &got_initial, &initial_ready]() mutable {
  fprintf(stderr, "[shmem] await-initial spawned on party\n");
  return Seq(i.PullServerInitialMetadata(),
       [&got_initial, &initial_ready](
           std::optional<ServerMetadataHandle> md) {
         fprintf(stderr, "[shmem] await-initial got md: %s\n",
           md.has_value() ? "yes" : "no");
         got_initial = std::move(md);
         initial_ready.Notify();
         return Empty{};
       });
      });
  for (int i = 0; i < 20000 && !initial_ready.HasBeenNotified(); ++i) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_initial.has_value());
  ASSERT_NE(got_initial.value(), nullptr);
  EXPECT_EQ(*got_initial.value()->get_pointer(ContentTypeMetadata()),
            ContentTypeMetadata::kApplicationGrpc);

  // Await server trailing metadata inside the party and notify this thread.
  std::optional<ServerMetadataHandle> got_trailing;
  Notification trailing_ready;
  call.initiator.SpawnInfallible(
      "await-trailing",
      [i = call.initiator, &got_trailing, &trailing_ready]() mutable {
        fprintf(stderr, "[shmem] await-trailing spawned on party\n");
        return Seq(i.PullServerTrailingMetadata(),
                   [&got_trailing, &trailing_ready](
                       ServerMetadataHandle md) {
                     fprintf(stderr, "[shmem] await-trailing got md\n");
                     got_trailing = std::move(md);
                     trailing_ready.Notify();
                     return Empty{};
                   });
      });
  for (int i = 0; i < 20000 && !trailing_ready.HasBeenNotified(); ++i) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_trailing.has_value());
  EXPECT_EQ(*got_trailing.value()->get_pointer(GrpcStatusMetadata()),
            GRPC_STATUS_UNIMPLEMENTED);

  // Explicitly destroy transports to help shutdown reader threads quickly.
  client.reset();
  server.reset();
}

TEST(ShmemE2E, UnaryEchoMessageThenUnimplemented) {
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

  auto rq = MakeResourceQuota("shmem-e2e");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("shmem-e2e-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  auto arena = call_arena_allocator->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

  auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
  md->Set(HttpPathMetadata(), Slice::FromExternalString("/echo"));
  auto call = MakeCallPair(std::move(md), std::move(arena));

  // Start transport call
  call.handler.SpawnInfallible(
      "start-call", [c = client.get(), h = call.handler]() mutable {
        c->client_transport()->StartCall(h.StartCall());
        return Empty{};
      });

  // Send a message, then finish sends
  const char* payload = "ping";
  call.initiator.SpawnInfallible("send-msg", [i = call.initiator, payload]() mutable {
    return Seq(i.PushMessage(Arena::MakePooled<Message>(
                   SliceBuffer(Slice::FromCopiedString(payload)), 0)),
               [i](StatusFlag s) mutable {
                 (void)s;
                 i.FinishSends();
                 return Empty{};
               });
  });

  // Wait for server initial metadata
  Notification got_imd;
  call.initiator.SpawnInfallible("await-imd", [i = call.initiator, &got_imd]() mutable {
    return Seq(i.PullServerInitialMetadata(), [&got_imd](auto) {
      got_imd.Notify();
      return Empty{};
    });
  });
  for (int i = 0; i < 20000 && !got_imd.HasBeenNotified(); ++i) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_imd.HasBeenNotified());

  // Expect an echoed message
  std::string echoed;
  Notification got_msg;
  call.initiator.SpawnInfallible("await-msg", [i = call.initiator, &echoed, &got_msg]() mutable {
    return Seq(i.PullMessage(), [&echoed, &got_msg](ServerToClientNextMessage m) {
      if (m.ok() && m.has_value()) {
        echoed = m.value().payload()->JoinIntoString();
        got_msg.Notify();
      }
      return Empty{};
    });
  });
  for (int i = 0; i < 20000 && !got_msg.HasBeenNotified(); ++i) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_msg.HasBeenNotified());
  EXPECT_EQ(echoed, "ping");

  // Wait for trailing metadata UNIMPLEMENTED
  std::optional<ServerMetadataHandle> trailing;
  Notification got_tr;
  call.initiator.SpawnInfallible("await-trailing", [i = call.initiator, &trailing, &got_tr]() mutable {
    return Seq(i.PullServerTrailingMetadata(), [&trailing, &got_tr](ServerMetadataHandle md) {
      trailing = std::move(md);
      got_tr.Notify();
      return Empty{};
    });
  });
  for (int i = 0; i < 20000 && !got_tr.HasBeenNotified(); ++i) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_tr.HasBeenNotified());
  ASSERT_TRUE(trailing.has_value());
  EXPECT_EQ(*trailing.value()->get_pointer(GrpcStatusMetadata()), GRPC_STATUS_UNIMPLEMENTED);

  client.reset();
  server.reset();
}

TEST(ShmemE2E, UnaryEchoTwoMessagesThenUnimplemented) {
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

  auto rq = MakeResourceQuota("shmem-e2e");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("shmem-e2e-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  auto arena = call_arena_allocator->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

  auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
  md->Set(HttpPathMetadata(), Slice::FromExternalString("/echo2"));
  auto call = MakeCallPair(std::move(md), std::move(arena));

  call.handler.SpawnInfallible(
      "start-call", [c = client.get(), h = call.handler]() mutable {
        c->client_transport()->StartCall(h.StartCall());
        return Empty{};
      });

  // Send two messages, then finish sends
  const char* p1 = "one";
  const char* p2 = "two";
  call.initiator.SpawnInfallible("send-two", [i = call.initiator, p1, p2]() mutable {
    return Seq(i.PushMessage(Arena::MakePooled<Message>(
                   SliceBuffer(Slice::FromCopiedString(p1)), 0)),
               [i, p2](StatusFlag) mutable {
                 return Seq(i.PushMessage(Arena::MakePooled<Message>(
                                SliceBuffer(Slice::FromCopiedString(p2)), 0)),
                            [i](StatusFlag) mutable {
                              i.FinishSends();
                              return Empty{};
                            });
               });
  });

  // Wait for initial md
  Notification got_imd;
  call.initiator.SpawnInfallible("await-imd", [i = call.initiator, &got_imd]() mutable {
    return Seq(i.PullServerInitialMetadata(), [&got_imd](auto) {
      got_imd.Notify();
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_imd.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_imd.HasBeenNotified());

  // Expect two echoed messages in order
  std::vector<std::string> echoed;
  Notification got_msg1, got_msg2;
  call.initiator.SpawnInfallible("await-msg1", [i = call.initiator, &echoed, &got_msg1]() mutable {
    return Seq(i.PullMessage(), [&echoed, &got_msg1](ServerToClientNextMessage m) {
      if (m.ok() && m.has_value()) {
        echoed.push_back(m.value().payload()->JoinIntoString());
        got_msg1.Notify();
      }
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_msg1.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_msg1.HasBeenNotified());

  call.initiator.SpawnInfallible("await-msg2", [i = call.initiator, &echoed, &got_msg2]() mutable {
    return Seq(i.PullMessage(), [&echoed, &got_msg2](ServerToClientNextMessage m) {
      if (m.ok() && m.has_value()) {
        echoed.push_back(m.value().payload()->JoinIntoString());
        got_msg2.Notify();
      }
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_msg2.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_msg2.HasBeenNotified());
  ASSERT_EQ(echoed.size(), 2u);
  EXPECT_EQ(echoed[0], "one");
  EXPECT_EQ(echoed[1], "two");

  // Trailing UNIMPLEMENTED
  std::optional<ServerMetadataHandle> trailing;
  Notification got_tr;
  call.initiator.SpawnInfallible("await-trailing", [i = call.initiator, &trailing, &got_tr]() mutable {
    return Seq(i.PullServerTrailingMetadata(), [&trailing, &got_tr](ServerMetadataHandle md) {
      trailing = std::move(md);
      got_tr.Notify();
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_tr.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_tr.HasBeenNotified());
  ASSERT_TRUE(trailing.has_value());
  EXPECT_EQ(*trailing.value()->get_pointer(GrpcStatusMetadata()), GRPC_STATUS_UNIMPLEMENTED);

  client.reset();
  server.reset();
}

TEST(ShmemE2E, UnaryCancelViaPayloadThenCancelled) {
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

  auto rq = MakeResourceQuota("shmem-e2e");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("shmem-e2e-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  auto arena = call_arena_allocator->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

  auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
  md->Set(HttpPathMetadata(), Slice::FromExternalString("/cancel"));
  auto call = MakeCallPair(std::move(md), std::move(arena));

  call.handler.SpawnInfallible(
      "start-call", [c = client.get(), h = call.handler]() mutable {
        c->client_transport()->StartCall(h.StartCall());
        return Empty{};
      });

  // Send a payload that the server interprets as a cancellation trigger, then finish sends.
  const char* payload = "cancel";
  call.initiator.SpawnInfallible("send-cancel", [i = call.initiator, payload]() mutable {
    return Seq(i.PushMessage(Arena::MakePooled<Message>(
                   SliceBuffer(Slice::FromCopiedString(payload)), 0)),
               [i](StatusFlag) mutable {
                 i.FinishSends();
                 return Empty{};
               });
  });

  // Wait for initial metadata
  Notification got_imd;
  call.initiator.SpawnInfallible("await-imd", [i = call.initiator, &got_imd]() mutable {
    return Seq(i.PullServerInitialMetadata(), [&got_imd](auto) {
      got_imd.Notify();
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_imd.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_imd.HasBeenNotified());

  // Expect trailing CANCELLED
  std::optional<ServerMetadataHandle> trailing;
  Notification got_tr;
  call.initiator.SpawnInfallible("await-trailing", [i = call.initiator, &trailing, &got_tr]() mutable {
    return Seq(i.PullServerTrailingMetadata(), [&trailing, &got_tr](ServerMetadataHandle md) {
      trailing = std::move(md);
      got_tr.Notify();
      return Empty{};
    });
  });
  for (int j = 0; j < 20000 && !got_tr.HasBeenNotified(); ++j) {
    ExecCtx::Get()->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(got_tr.HasBeenNotified());
  ASSERT_TRUE(trailing.has_value());
  EXPECT_EQ(*trailing.value()->get_pointer(GrpcStatusMetadata()), GRPC_STATUS_CANCELLED);

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
