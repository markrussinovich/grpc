//
// gRPC shmem transport size sweep using v3 promise-based stack.
// Use Google Benchmark harness; recommend running with:
//   --benchmark_out=shmem_sweep.json --benchmark_out_format=json
//

#include <benchmark/benchmark.h>

#include "absl/log/check.h"
#include "src/core/call/call_arena_allocator.h"
#include "src/core/call/call_spine.h"
#include "src/core/config/core_configuration.h"
#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "include/grpc/event_engine/event_engine.h"
#include "src/core/lib/promise/seq.h"
#include "src/core/lib/resource_quota/memory_quota.h"
#include "src/core/lib/resource_quota/resource_quota.h"
#include "src/core/util/notification.h"

using namespace grpc_core;

static void BM_ShmemPromiseUnaryPingPong(benchmark::State& state) {
  const int req_bytes = static_cast<int>(state.range(0));
  const int resp_bytes = static_cast<int>(state.range(1));
  ExecCtx exec_ctx;

  // Create and cache the transport pair and allocator once per process.
  struct Env {
    OrphanablePtr<Transport> client;
    OrphanablePtr<Transport> server;
    RefCountedPtr<CallArenaAllocator> call_arena_allocator;
  RefCountedPtr<UnstartedCallDestination> dest;
  };
  static Env* env = [] {
    auto* e = new Env();
    ChannelArgs args = CoreConfiguration::Get()
                           .channel_args_preconditioning()
                           .PreconditionChannelArgs(nullptr);
    auto pair = MakeShmemTransportPair(args);
    e->client = std::move(pair.first);
    e->server = std::move(pair.second);
  class ServerCallDestination : public UnstartedCallDestination {
     public:
      void StartCall(UnstartedCallHandler h) override { (void)h.StartCall(); }
      void Orphaned() override {}
  };
  e->dest = MakeRefCounted<ServerCallDestination>();
  e->server->server_transport()->SetCallDestination(e->dest);
    auto rq = MakeResourceQuota("bm-shmem");
    auto allocator = rq->memory_quota()->CreateMemoryAllocator("bm-shmem-alloc");
    e->call_arena_allocator = MakeRefCounted<CallArenaAllocator>(std::move(allocator), 1024);
    return e;
  }();

  // Prebuild payload for this size once outside the loop.
  std::string payload;
  if (req_bytes > 0) payload.assign(req_bytes, 'q');

  for (auto _ : state) {
    auto arena = env->call_arena_allocator->MakeArena();
    auto ee = grpc_event_engine::experimental::GetDefaultEventEngine();
    arena->SetContext<grpc_event_engine::experimental::EventEngine>(ee.get());

    auto md = Arena::MakePooledForOverwrite<ClientMetadata>();
    // Use benchmark path so the transport returns OK status for successful completion.
    md->Set(HttpPathMetadata(), Slice::FromExternalString("/benchmark"));
    auto call = MakeCallPair(std::move(md), std::move(arena));

    call.handler.SpawnInfallible("start-call",
                                 [c = env->client.get(), h = call.handler]() mutable {
                                   c->client_transport()->StartCall(h.StartCall());
                                   return Empty{};
                                 });

    if (req_bytes > 0) {
      const std::string* payload_ptr = &payload;
      call.initiator.SpawnInfallible(
          "send-msg", [i = call.initiator, payload_ptr]() mutable {
            return Seq(i.PushMessage(Arena::MakePooled<Message>(
                           SliceBuffer(Slice::FromCopiedString(*payload_ptr)), 0)),
                       [i](StatusFlag) mutable {
                         i.FinishSends();
                         return Empty{};
                       });
          });
    } else {
      call.initiator.SpawnFinishSends();
    }

    // Pull initial metadata but don't wait on it; it will complete before trailing.
    call.initiator.SpawnInfallible("imd", [i = call.initiator]() mutable {
      return Seq(i.PullServerInitialMetadata(), [](auto) { return Empty{}; });
    });

    // Pull one message if we expect one, but don't wait separately.
    if (req_bytes > 0) {
      call.initiator.SpawnInfallible("msg", [i = call.initiator]() mutable {
        return Seq(i.PullMessage(), [](ServerToClientNextMessage) { return Empty{}; });
      });
    }

    // Wait only on trailing metadata as the barrier for completion.
    Notification tr;
    call.initiator.SpawnInfallible("tr", [i = call.initiator, &tr]() mutable {
      return Seq(i.PullServerTrailingMetadata(), [&tr](ServerMetadataHandle) {
        tr.Notify();
        return Empty{};
      });
    });
  tr.WaitForNotification();
  }

  state.SetBytesProcessed(
      (static_cast<int64_t>(req_bytes) + resp_bytes) * state.iterations());
}

static void SweepSizesArgs(benchmark::internal::Benchmark* b) {
  b->Args({0, 0});
  for (int i = 1; i <= 8 * 1024 * 1024; i *= 8) {
    b->Args({i, 0});
    b->Args({0, i});
    b->Args({i, i});
  }
}

BENCHMARK(BM_ShmemPromiseUnaryPingPong)->Apply(SweepSizesArgs);

namespace benchmark {
void RunTheBenchmarksNamespaced() { RunSpecifiedBenchmarks(); }
}

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  benchmark::RunTheBenchmarksNamespaced();
  return 0;
}
