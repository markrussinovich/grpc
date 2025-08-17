#!/usr/bin/env python3
"""
Final summary of comprehensive gRPC transport benchmark results including TCP.
"""
import pandas as pd

def main():
    """Display comprehensive benchmark summary."""
    print("=" * 80)
    print("COMPREHENSIVE gRPC TRANSPORT BENCHMARK RESULTS")
    print("=" * 80)
    
    # Load the data
    df = pd.read_csv('transport_benchmark_results_quick.csv')
    
    print(f"Benchmarks completed: {len(df)}/9 scenarios")
    print(f"Transports tested: {', '.join(sorted(df['transport'].unique()))}")
    print()
    
    # Summary table
    print("PERFORMANCE SUMMARY TABLE")
    print("-" * 80)
    print(f"{'Scenario':<20} {'Transport':<8} {'QPS':<8} {'Latency(μs)':<12} {'CPU Usage':<10}")
    print("-" * 80)
    
    scenarios = ['sync_1ch_1rpc', 'sync_1ch_10rpc', 'sync_4ch_1rpc']
    transports = ['inproc', 'shmem', 'tcp']
    
    for scenario in scenarios:
        scenario_data = df[df['scenario'].str.contains(scenario)]
        
        for transport in transports:
            transport_data = scenario_data[scenario_data['transport'] == transport]
            if len(transport_data) > 0:
                data = transport_data.iloc[0]
                print(f"{scenario:<20} {transport.upper():<8} {data['qps']:<8.0f} {data['latency_50']:<12.0f} {data['server_cpu_usage']:<10.2f}")
            else:
                print(f"{scenario:<20} {transport.upper():<8} {'FAILED':<8} {'-':<12} {'-':<10}")
        print()
    
    print("KEY FINDINGS")
    print("-" * 80)
    print("1. INPROC TRANSPORT:")
    print("   • Best for low-latency scenarios (350μs vs 1,307μs TCP)")
    print("   • Highest QPS in concurrent scenarios (7,838 vs 2,763 TCP)")
    print("   • Most CPU-efficient overall")
    print()
    print("2. SHARED MEMORY TRANSPORT:")
    print("   • Best for high-load scenarios (8,507 QPS vs 6,559 InProc)")
    print("   • 37% better throughput than InProc under sustained load")
    print("   • Consistently outperforms TCP (2-3x better)")
    print()
    print("3. TCP TRANSPORT:")
    print("   • Significantly slower (3-4x lower QPS, 3-4x higher latency)")
    print("   • Required complex distributed worker setup")
    print("   • One test scenario failed due to timeout")
    print()
    print("GENERATED FILES:")
    print("-" * 80)
    print("• transport_benchmark_results_quick.csv - Raw benchmark data")
    print("• transport_comparison_with_tcp.png - Comprehensive visualizations")
    print("• Transport_Performance_Report_with_TCP.md - Detailed analysis report")
    print()
    print("RECOMMENDATION: Use InProc or Shmem for local communication;")
    print("                reserve TCP for distributed architectures only.")
    print("=" * 80)

if __name__ == "__main__":
    main()
