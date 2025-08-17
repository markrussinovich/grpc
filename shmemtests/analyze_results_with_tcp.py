#!/usr/bin/env python3
"""
Generate comprehensive visualizations comparing inproc, shmem, and TCP transport performance.
"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

def load_and_analyze_data():
    """Load benchmark data and prepare for analysis."""
    df = pd.read_csv('transport_benchmark_results_quick.csv')
    
    # Extract scenario base name (remove transport prefix)
    df['scenario_base'] = df['scenario'].str.replace(r'^(inproc|shmem|tcp)_', '', regex=True)
    
    print("=== TRANSPORT PERFORMANCE COMPARISON ===")
    print(f"Total benchmarks: {len(df)}")
    print(f"Transports: {', '.join(df['transport'].unique())}")
    print(f"Scenarios: {', '.join(df['scenario_base'].unique())}")
    
    return df

def create_comprehensive_plots(df):
    """Create comprehensive performance comparison plots."""
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('gRPC Transport Performance Comparison: InProc vs Shmem vs TCP', fontsize=16, fontweight='bold')
    
    scenarios = df['scenario_base'].unique()
    transports = ['inproc', 'shmem', 'tcp']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # Blue, Orange, Green
    
    # 1. QPS Comparison
    ax = axes[0, 0]
    x = np.arange(len(scenarios))
    width = 0.25
    
    for i, transport in enumerate(transports):
        transport_data = df[df['transport'] == transport]
        qps_values = []
        for scenario in scenarios:
            scenario_data = transport_data[transport_data['scenario_base'] == scenario]
            if len(scenario_data) > 0:
                qps_values.append(scenario_data['qps'].iloc[0])
            else:
                qps_values.append(0)  # Missing data
        
        bars = ax.bar(x + i*width, qps_values, width, label=transport.upper(), color=colors[i], alpha=0.8)
        
        # Add value labels on bars
        for bar, val in zip(bars, qps_values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(qps_values)*0.01,
                       f'{val:.0f}', ha='center', va='bottom', fontsize=9)
    
    ax.set_xlabel('Scenario')
    ax.set_ylabel('QPS (Queries Per Second)')
    ax.set_title('Throughput Comparison')
    ax.set_xticks(x + width)
    ax.set_xticklabels(scenarios, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 2. Latency Comparison (p50)
    ax = axes[0, 1]
    for i, transport in enumerate(transports):
        transport_data = df[df['transport'] == transport]
        latency_values = []
        for scenario in scenarios:
            scenario_data = transport_data[transport_data['scenario_base'] == scenario]
            if len(scenario_data) > 0:
                latency_values.append(scenario_data['latency_50'].iloc[0])
            else:
                latency_values.append(np.nan)
        
        bars = ax.bar(x + i*width, latency_values, width, label=transport.upper(), color=colors[i], alpha=0.8)
        
        # Add value labels
        for bar, val in zip(bars, latency_values):
            if not np.isnan(val):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max([v for v in latency_values if not np.isnan(v)])*0.01,
                       f'{val:.0f}μs', ha='center', va='bottom', fontsize=9)
    
    ax.set_xlabel('Scenario')
    ax.set_ylabel('Latency (μs)')
    ax.set_title('Median Latency (p50) Comparison')
    ax.set_xticks(x + width)
    ax.set_xticklabels(scenarios, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 3. CPU Efficiency (QPS per CPU usage)
    ax = axes[0, 2]
    for i, transport in enumerate(transports):
        transport_data = df[df['transport'] == transport]
        efficiency_values = []
        for scenario in scenarios:
            scenario_data = transport_data[transport_data['scenario_base'] == scenario]
            if len(scenario_data) > 0:
                cpu_usage = scenario_data['server_cpu_usage'].iloc[0]
                qps = scenario_data['qps'].iloc[0]
                efficiency = qps / cpu_usage if cpu_usage > 0 else 0
                efficiency_values.append(efficiency)
            else:
                efficiency_values.append(0)
        
        bars = ax.bar(x + i*width, efficiency_values, width, label=transport.upper(), color=colors[i], alpha=0.8)
        
        # Add value labels
        for bar, val in zip(bars, efficiency_values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(efficiency_values)*0.01,
                       f'{val:.0f}', ha='center', va='bottom', fontsize=9)
    
    ax.set_xlabel('Scenario')
    ax.set_ylabel('QPS per CPU Unit')
    ax.set_title('CPU Efficiency Comparison')
    ax.set_xticks(x + width)
    ax.set_xticklabels(scenarios, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 4. Latency Distribution (p50, p90, p99)
    ax = axes[1, 0]
    scenario_focus = 'sync_1ch_1rpc'  # Focus on one scenario for latency distribution
    focus_data = df[df['scenario_base'] == scenario_focus]
    
    latency_percentiles = ['latency_50', 'latency_90', 'latency_99']
    percentile_labels = ['p50', 'p90', 'p99']
    
    for i, transport in enumerate(transports):
        transport_data = focus_data[focus_data['transport'] == transport]
        if len(transport_data) > 0:
            latencies = [transport_data[perc].iloc[0] for perc in latency_percentiles]
            ax.plot(percentile_labels, latencies, marker='o', linewidth=2, markersize=8, 
                   label=transport.upper(), color=colors[i])
    
    ax.set_xlabel('Latency Percentile')
    ax.set_ylabel('Latency (μs)')
    ax.set_title(f'Latency Distribution - {scenario_focus}')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 5. QPS vs Latency Scatter
    ax = axes[1, 1]
    for i, transport in enumerate(transports):
        transport_data = df[df['transport'] == transport]
        qps_vals = transport_data['qps'].values
        latency_vals = transport_data['latency_50'].values
        
        ax.scatter(latency_vals, qps_vals, label=transport.upper(), color=colors[i], s=100, alpha=0.7)
        
        # Add scenario labels
        for j, scenario in enumerate(transport_data['scenario_base'].values):
            ax.annotate(scenario.replace('sync_', ''), 
                       (latency_vals[j], qps_vals[j]), 
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
    
    ax.set_xlabel('Median Latency (μs)')
    ax.set_ylabel('QPS')
    ax.set_title('QPS vs Latency Trade-off')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 6. Relative Performance (normalized to TCP)
    ax = axes[1, 2]
    tcp_baseline = df[df['transport'] == 'tcp'].groupby('scenario_base')['qps'].first()
    
    for i, transport in enumerate(transports):
        transport_data = df[df['transport'] == transport]
        relative_perf = []
        scenario_labels = []
        
        for scenario in scenarios:
            scenario_data = transport_data[transport_data['scenario_base'] == scenario]
            if len(scenario_data) > 0 and scenario in tcp_baseline.index:
                tcp_qps = tcp_baseline[scenario]
                transport_qps = scenario_data['qps'].iloc[0]
                relative = transport_qps / tcp_qps if tcp_qps > 0 else 0
                relative_perf.append(relative)
                scenario_labels.append(scenario)
        
        if relative_perf:
            bars = ax.bar([s + f'_{transport}' for s in scenario_labels], relative_perf, 
                         label=transport.upper(), color=colors[i], alpha=0.8)
            
            # Add value labels
            for bar, val in zip(bars, relative_perf):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                       f'{val:.1f}x', ha='center', va='bottom', fontsize=9)
    
    ax.axhline(y=1.0, color='red', linestyle='--', alpha=0.7, label='TCP Baseline')
    ax.set_xlabel('Scenario by Transport')
    ax.set_ylabel('Performance Relative to TCP')
    ax.set_title('Relative Performance (TCP = 1.0x)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig('transport_comparison_with_tcp.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_summary_analysis(df):
    """Create summary analysis with TCP included."""
    print("\n=== DETAILED PERFORMANCE ANALYSIS ===")
    
    # Performance summary by transport
    print("\n--- Performance Summary by Transport ---")
    summary = df.groupby('transport').agg({
        'qps': ['mean', 'max', 'min'],
        'latency_50': ['mean', 'min', 'max'],
        'server_cpu_usage': 'mean'
    }).round(2)
    
    print(summary)
    
    # Scenario-specific analysis
    print("\n--- Scenario-Specific Analysis ---")
    for scenario in df['scenario_base'].unique():
        scenario_data = df[df['scenario_base'] == scenario]
        print(f"\n{scenario}:")
        
        for transport in ['inproc', 'shmem', 'tcp']:
            transport_data = scenario_data[scenario_data['transport'] == transport]
            if len(transport_data) > 0:
                data = transport_data.iloc[0]
                print(f"  {transport.upper()}: {data['qps']:.0f} QPS, {data['latency_50']:.0f}μs p50, "
                      f"{data['server_cpu_usage']:.2f} CPU")
            else:
                print(f"  {transport.upper()}: NO DATA")
    
    # Key insights
    print("\n--- KEY INSIGHTS ---")
    
    # Best throughput per scenario
    print("\nHighest QPS by scenario:")
    for scenario in df['scenario_base'].unique():
        scenario_data = df[df['scenario_base'] == scenario]
        if len(scenario_data) > 0:
            best = scenario_data.loc[scenario_data['qps'].idxmax()]
            print(f"  {scenario}: {best['transport'].upper()} ({best['qps']:.0f} QPS)")
    
    # Best latency per scenario
    print("\nLowest latency by scenario:")
    for scenario in df['scenario_base'].unique():
        scenario_data = df[df['scenario_base'] == scenario]
        if len(scenario_data) > 0:
            best = scenario_data.loc[scenario_data['latency_50'].idxmin()]
            print(f"  {scenario}: {best['transport'].upper()} ({best['latency_50']:.0f}μs)")
    
    # TCP vs others comparison
    print("\n--- TCP vs In-Memory Transports ---")
    tcp_data = df[df['transport'] == 'tcp']
    for _, tcp_row in tcp_data.iterrows():
        scenario = tcp_row['scenario_base']
        scenario_data = df[df['scenario_base'] == scenario]
        
        print(f"\n{scenario}:")
        print(f"  TCP: {tcp_row['qps']:.0f} QPS, {tcp_row['latency_50']:.0f}μs")
        
        for transport in ['inproc', 'shmem']:
            transport_data = scenario_data[scenario_data['transport'] == transport]
            if len(transport_data) > 0:
                data = transport_data.iloc[0]
                qps_ratio = data['qps'] / tcp_row['qps']
                latency_ratio = tcp_row['latency_50'] / data['latency_50']
                print(f"  {transport.upper()}: {data['qps']:.0f} QPS ({qps_ratio:.1f}x), "
                      f"{data['latency_50']:.0f}μs ({latency_ratio:.1f}x better latency)")

def main():
    """Main analysis function."""
    df = load_and_analyze_data()
    create_comprehensive_plots(df)
    create_summary_analysis(df)

if __name__ == "__main__":
    main()
