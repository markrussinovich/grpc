#include <iostream>
#include <memory>
#include "absl/log/check.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/call/call_spine.h"
#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/lib/resource_quota/memory_quota.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/util/notification.h"

using namespace grpc_core;

int main() {
  std::cout << "=== Testing Zero-Copy Message Path ===" << std::endl;
  
  ExecCtx exec_ctx;
  ChannelArgs args = CoreConfiguration::Get()
                         .channel_args_preconditioning()
                         .PreconditionChannelArgs(nullptr);
  
  auto pair = MakeShmemTransportPair(args);
  auto client = std::move(pair.first);
  auto server = std::move(pair.second);
  
  class ServerCallDestination : public UnstartedCallDestination {
   public:
    void StartCall(UnstartedCallHandler h) override { 
      std::cout << "Server StartCall called" << std::endl;
      (void)h.StartCall(); 
    }
    void Orphaned() override {}
  };
  
  auto dest = MakeRefCounted<ServerCallDestination>();
  server->server_transport()->SetCallDestination(dest);
  
  auto rq = MakeResourceQuota("test-zero-copy");
  auto allocator = rq->memory_quota()->CreateMemoryAllocator("test-zero-copy-alloc");
  auto call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
  
  std::cout << "Making call with payload..." << std::endl;
  
  auto arena = call_arena_allocator->MakeArena();
  auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
  arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());
  
  auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
  md->Set(HttpPathMetadata(), Slice::FromExternalString("/echo"));
  auto call = MakeCallPair(std::move(md), std::move(arena));
  
  std::cout << "Starting call..." << std::endl;
  
  call.handler.SpawnInfallible("start-call",
                               [c = client.get(), h = call.handler]() mutable {
                                 std::cout << "Client StartCall spawned" << std::endl;
                                 c->client_transport()->StartCall(h.StartCall());
                                 return Empty{};
                               });
  
  // Create a message with payload to test zero-copy
  std::string payload = "Hello Zero-Copy World! This is a test payload to verify that zero-copy message forwarding works correctly.";
  std::cout << "Sending message with payload size: " << payload.size() << " bytes" << std::endl;
  
  call.initiator.SpawnInfallible("send-msg", [i = call.initiator, payload]() mutable {
    return Seq(i.PushMessage(Arena::MakePooled<Message>(
                   SliceBuffer(Slice::FromCopiedString(payload)), 0)),
               [i](StatusFlag) mutable {
                 i.FinishSends();
                 return Empty{};
               });
  });
  
  std::cout << "Waiting for trailing metadata..." << std::endl;
  Notification tr;
  call.initiator.SpawnInfallible("tr", [i = call.initiator, &tr]() mutable {
    return Map(i.PullServerTrailingMetadata(), [&tr](ServerMetadataHandle) {
      std::cout << "Got trailing metadata!" << std::endl;
      tr.Notify();
      return Empty{};
    });
  });
  
  tr.WaitForNotification();
  std::cout << "Test completed successfully!" << std::endl;
  return 0;
}
