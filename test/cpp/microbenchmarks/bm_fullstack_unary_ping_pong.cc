//
//
// Copyright 2016 gRPC authors.
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
//
//

// Benchmark gRPC end2end in various configurations

#include "test/core/test_util/test_config.h"
#include "test/cpp/microbenchmarks/fullstack_unary_ping_pong.h"
#include "test/cpp/util/test_config.h"
#include <atomic>
#include <thread>
#include <chrono>

// Heartbeat globals (definition) placed inside grpc::testing namespace so that
// extern declarations in the shared header resolve correctly at link time.
namespace grpc {
namespace testing {
std::atomic<int64_t> g_benchmark_last_progress_ns{0};
std::atomic<uint64_t> g_benchmark_iteration_counter{0};
// Exposed (non-static) so header extern NowSteadyNanos() can link here.
int64_t NowSteadyNanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
            .count();
}
}  // namespace testing
}  // namespace grpc

// Bench run active flag only used internally in this TU.
static std::atomic<bool> g_bench_run_active{false};

static void ProgressPrinterThread(int interval_sec) {
    const char* enable_env = std::getenv("GRPC_BENCH_PROGRESS");
    if (enable_env && std::string(enable_env) == "0") return; // disabled
    int64_t start = grpc::testing::NowSteadyNanos();
    int last_reported_stall_secs = -1;
    while (g_bench_run_active.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_sec));
        if (!g_bench_run_active.load(std::memory_order_relaxed)) break;
        int64_t now = grpc::testing::NowSteadyNanos();
        int64_t last = grpc::testing::g_benchmark_last_progress_ns.load(std::memory_order_relaxed);
        uint64_t iters = grpc::testing::g_benchmark_iteration_counter.load(std::memory_order_relaxed);
        double elapsed_s = (now - start) / 1e9;
        double since_last_s = (now - last) / 1e9;
        // Print a heartbeat line to stderr so Bazel surfaces it even if stdout buffered.
        fprintf(stderr,
                        "[bench-progress] t=%.1fs since_start, last_progress=%.2fs ago, total_inner_iters=%llu\n",
                        elapsed_s, since_last_s, (unsigned long long)iters);
        fflush(stderr);
        // If no progress for > 30s (configurable via env), flag a potential stall.
        int stall_threshold = 30;
        if (const char* thr = std::getenv("GRPC_BENCH_STALL_SECS")) {
            stall_threshold = atoi(thr);
            if (stall_threshold <= 0) stall_threshold = 30;
        }
        if (since_last_s > stall_threshold && (int)since_last_s != last_reported_stall_secs) {
            last_reported_stall_secs = (int)since_last_s;
            fprintf(stderr,
                            "[bench-progress][WARN] No inner-loop progress for %.0f seconds (threshold=%ds)."\
                            " Benchmark may be blocked.\n",
                            since_last_s, stall_threshold);
            fflush(stderr);
        }
    }
    fprintf(stderr, "[bench-progress] benchmark run completed.\n");
    fflush(stderr);
}

namespace grpc {
namespace testing {

//******************************************************************************
// CONFIGURATIONS
//

// Replace "benchmark::internal::Benchmark" with "::testing::Benchmark" to use
// internal microbenchmarking tooling
static void SweepSizesArgs(benchmark::internal::Benchmark* b) {
  b->Args({0, 0});
  for (int i = 1; i <= 128 * 1024 * 1024; i *= 8) {
    b->Args({i, 0});
    b->Args({0, i});
    b->Args({i, i});
  }
}

BENCHMARK_TEMPLATE(BM_UnaryPingPong, TCP, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, MinTCP, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, UDS, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, MinUDS, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, MinInProcess, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, ShmemTransport, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, MinShmemTransport, NoOpMutator, NoOpMutator)
    ->Apply(SweepSizesArgs);
BENCHMARK_TEMPLATE(BM_UnaryPingPong, SockPair, NoOpMutator, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, MinSockPair, NoOpMutator, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<10>, 1>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<31>, 1>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<100>, 1>,
                   NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<10>, 2>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<31>, 2>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomBinaryMetadata<100>, 2>,
                   NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomBinaryMetadata<10>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomBinaryMetadata<31>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomBinaryMetadata<100>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomAsciiMetadata<10>, 1>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomAsciiMetadata<31>, 1>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess,
                   Client_AddMetadata<RandomAsciiMetadata<100>, 1>, NoOpMutator)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomAsciiMetadata<10>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomAsciiMetadata<31>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomAsciiMetadata<100>, 1>)
    ->Args({0, 0});
BENCHMARK_TEMPLATE(BM_UnaryPingPong, InProcess, NoOpMutator,
                   Server_AddInitialMetadata<RandomAsciiMetadata<10>, 100>)
    ->Args({0, 0});

}  // namespace testing
}  // namespace grpc

// Some distros have RunSpecifiedBenchmarks under the benchmark namespace,
// and others do not. This allows us to support both modes.
namespace benchmark {
void RunTheBenchmarksNamespaced() { RunSpecifiedBenchmarks(); }
}  // namespace benchmark

int main(int argc, char** argv) {
    grpc::testing::TestEnvironment env(&argc, argv);
    LibraryInitializer libInit;
    ::benchmark::Initialize(&argc, argv);
    grpc::testing::InitTest(&argc, &argv, false);
    grpc::testing::g_benchmark_last_progress_ns.store(grpc::testing::NowSteadyNanos(), std::memory_order_relaxed);
    grpc::testing::g_benchmark_iteration_counter.store(0, std::memory_order_relaxed);
    g_bench_run_active.store(true, std::memory_order_relaxed);
    int interval = 10;  // seconds
    if (const char* iv = std::getenv("GRPC_BENCH_PROGRESS_INTERVAL")) {
        int v = atoi(iv); if (v > 0 && v < 3600) interval = v;
    }
    std::thread progress_thread(ProgressPrinterThread, interval);
    benchmark::RunTheBenchmarksNamespaced();
    g_bench_run_active.store(false, std::memory_order_relaxed);
    if (progress_thread.joinable()) progress_thread.join();
    return 0;
}
