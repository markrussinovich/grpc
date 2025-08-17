#!/usr/bin/env python3
"""
Quick transport benchmark focusing on key scenarios for comparison.
"""
import json
import subprocess
import pandas as pd
import os
from benchmark_transports import TransportBenchmark

def main():
    benchmark = TransportBenchmark()
    results = []
    
    # Key scenarios to test
    scenarios = [
        ("sync_1ch_1rpc", "SYNC_CLIENT", "SYNC_SERVER", 1, 1, 1),
        ("sync_1ch_10rpc", "SYNC_CLIENT", "SYNC_SERVER", 10, 1, 1),
        ("sync_4ch_1rpc", "SYNC_CLIENT", "SYNC_SERVER", 1, 4, 1),
    ]
    
    transports = ["inproc", "shmem", "tcp"]
    
    for transport in transports:
        print(f"\n=== Running {transport.upper()} Transport Benchmarks ===")
        for scenario_name, client_type, server_type, outstanding_rpcs, channels, threads in scenarios:
            scenario_config = benchmark.create_scenario(
                f"{transport}_{scenario_name}",
                client_type, server_type, channels, outstanding_rpcs, threads
            )
            
            metrics = benchmark.run_benchmark(transport, scenario_config)
            if metrics:
                results.append(metrics)
                print(f"  {scenario_name}: {metrics['qps']:.2f} QPS, {metrics['latency_50']:.2f}μs p50")
            else:
                print(f"  {scenario_name}: FAILED")
    
    # Save results
    if results:
        df = pd.DataFrame(results)
        df.to_csv('transport_benchmark_results_quick.csv', index=False)
        print(f"\nSaved {len(results)} results to transport_benchmark_results_quick.csv")
        return df
    else:
        print("\nNo results to save!")
        return None

if __name__ == "__main__":
    main()
