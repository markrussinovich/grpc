#!/usr/bin/env python3
"""
Analysis and visualization of inproc vs shmem transport benchmark results.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

def load_and_analyze_data():
    """Load benchmark data and perform analysis."""
    
    # Read the CSV data
    df = pd.read_csv('transport_benchmark_results.csv')
    
    # Extract scenario base names
    df['scenario_base'] = df['scenario'].str.replace(r'^(inproc|shmem)_', '', regex=True)
    
    # Print summary statistics
    print("=== TRANSPORT PERFORMANCE COMPARISON SUMMARY ===\n")
    
    for transport in ['inproc', 'shmem']:
        transport_data = df[df['transport'] == transport]
        print(f"{transport.upper()} Transport Results:")
        print(f"  Average QPS: {transport_data['qps'].mean():.2f}")
        print(f"  Average P50 Latency: {transport_data['latency_50'].mean():.2f} μs")
        print(f"  Average P99 Latency: {transport_data['latency_99'].mean():.2f} μs")
        print(f"  Average CPU Usage: {transport_data['server_cpu_usage'].mean():.2f}")
        print()
    
    # Calculate relative performance
    print("=== RELATIVE PERFORMANCE (shmem vs inproc) ===\n")
    
    for scenario in df['scenario_base'].unique():
        inproc_data = df[(df['transport'] == 'inproc') & (df['scenario_base'] == scenario)]
        shmem_data = df[(df['transport'] == 'shmem') & (df['scenario_base'] == scenario)]
        
        if len(inproc_data) > 0 and len(shmem_data) > 0:
            qps_ratio = shmem_data['qps'].iloc[0] / inproc_data['qps'].iloc[0]
            lat_ratio = shmem_data['latency_50'].iloc[0] / inproc_data['latency_50'].iloc[0]
            
            print(f"{scenario}:")
            print(f"  QPS ratio (shmem/inproc): {qps_ratio:.3f}")
            print(f"  Latency ratio (shmem/inproc): {lat_ratio:.3f}")
            print(f"  Performance: {'SHMEM FASTER' if qps_ratio > 1 else 'INPROC FASTER'}")
            print()
    
    return df

def create_comprehensive_plots(df):
    """Create comprehensive visualization plots."""
    
    # Create a large figure with multiple subplots
    fig = plt.figure(figsize=(20, 16))
    
    # Extract scenario names for x-axis
    scenarios = df['scenario_base'].unique()
    x = np.arange(len(scenarios))
    width = 0.35
    
    # Colors for transports
    colors = {'inproc': '#2E86C1', 'shmem': '#E74C3C'}
    
    # 1. QPS Comparison (Top Left)
    ax1 = plt.subplot(3, 3, 1)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        qps_values = [transport_data[transport_data['scenario_base'] == s]['qps'].iloc[0] 
                     if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                     for s in scenarios]
        ax1.bar(x + i*width, qps_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax1.set_xlabel('Scenario')
    ax1.set_ylabel('QPS')
    ax1.set_title('Queries Per Second (QPS)', fontweight='bold')
    ax1.set_xticks(x + width/2)
    ax1.set_xticklabels(scenarios, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        qps_values = [transport_data[transport_data['scenario_base'] == s]['qps'].iloc[0] 
                     if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                     for s in scenarios]
        for j, v in enumerate(qps_values):
            ax1.text(j + i*width, v + max(qps_values)*0.01, f'{v:.0f}', 
                    ha='center', va='bottom', fontsize=8)
    
    # 2. Latency P50 Comparison (Top Center)
    ax2 = plt.subplot(3, 3, 2)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        lat_values = [transport_data[transport_data['scenario_base'] == s]['latency_50'].iloc[0] 
                     if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                     for s in scenarios]
        ax2.bar(x + i*width, lat_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax2.set_xlabel('Scenario')
    ax2.set_ylabel('Latency (μs)')
    ax2.set_title('Median Latency (P50)', fontweight='bold')
    ax2.set_xticks(x + width/2)
    ax2.set_xticklabels(scenarios, rotation=45, ha='right')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Latency P99 Comparison (Top Right)
    ax3 = plt.subplot(3, 3, 3)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        lat_values = [transport_data[transport_data['scenario_base'] == s]['latency_99'].iloc[0] 
                     if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                     for s in scenarios]
        ax3.bar(x + i*width, lat_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax3.set_xlabel('Scenario')
    ax3.set_ylabel('Latency (μs)')
    ax3.set_title('P99 Latency', fontweight='bold')
    ax3.set_xticks(x + width/2)
    ax3.set_xticklabels(scenarios, rotation=45, ha='right')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. CPU Usage Comparison (Middle Left)
    ax4 = plt.subplot(3, 3, 4)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        cpu_values = [transport_data[transport_data['scenario_base'] == s]['server_cpu_usage'].iloc[0] 
                     if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                     for s in scenarios]
        ax4.bar(x + i*width, cpu_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax4.set_xlabel('Scenario')
    ax4.set_ylabel('CPU Usage')
    ax4.set_title('Server CPU Usage', fontweight='bold')
    ax4.set_xticks(x + width/2)
    ax4.set_xticklabels(scenarios, rotation=45, ha='right')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # 5. QPS per Core Comparison (Middle Center)
    ax5 = plt.subplot(3, 3, 5)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        qps_core_values = [transport_data[transport_data['scenario_base'] == s]['qps_per_core'].iloc[0] 
                          if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                          for s in scenarios]
        ax5.bar(x + i*width, qps_core_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax5.set_xlabel('Scenario')
    ax5.set_ylabel('QPS per Core')
    ax5.set_title('QPS per Server Core', fontweight='bold')
    ax5.set_xticks(x + width/2)
    ax5.set_xticklabels(scenarios, rotation=45, ha='right')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    
    # 6. Efficiency Comparison (Middle Right)
    ax6 = plt.subplot(3, 3, 6)
    for i, transport in enumerate(['inproc', 'shmem']):
        transport_data = df[df['transport'] == transport]
        efficiency_values = []
        for s in scenarios:
            scenario_data = transport_data[transport_data['scenario_base'] == s]
            if len(scenario_data) > 0:
                qps = scenario_data['qps'].iloc[0]
                cpu = scenario_data['server_cpu_usage'].iloc[0]
                efficiency = qps / cpu if cpu > 0 else 0
                efficiency_values.append(efficiency)
            else:
                efficiency_values.append(0)
        ax6.bar(x + i*width, efficiency_values, width, label=transport, color=colors[transport], alpha=0.8)
    
    ax6.set_xlabel('Scenario')
    ax6.set_ylabel('QPS / CPU Usage')
    ax6.set_title('Efficiency (QPS per CPU Usage)', fontweight='bold')
    ax6.set_xticks(x + width/2)
    ax6.set_xticklabels(scenarios, rotation=45, ha='right')
    ax6.legend()
    ax6.grid(True, alpha=0.3)
    
    # 7. Latency Distribution for sync_1ch_1rpc (Bottom Left)
    ax7 = plt.subplot(3, 3, 7)
    test_scenario = 'sync_1ch_1rpc'
    scenario_data = df[df['scenario_base'] == test_scenario]
    
    percentiles = ['50', '90', '95', '99', '999']
    percentile_labels = ['P50', 'P90', 'P95', 'P99', 'P99.9']
    
    for transport in ['inproc', 'shmem']:
        transport_data = scenario_data[scenario_data['transport'] == transport]
        if len(transport_data) > 0:
            latencies = [transport_data[f'latency_{p}'].iloc[0] for p in percentiles]
            ax7.plot(percentile_labels, latencies, marker='o', linewidth=2, 
                    label=transport, color=colors[transport])
    
    ax7.set_xlabel('Latency Percentile')
    ax7.set_ylabel('Latency (μs)')
    ax7.set_title(f'Latency Distribution - {test_scenario}', fontweight='bold')
    ax7.legend()
    ax7.grid(True, alpha=0.3)
    
    # 8. Performance Ratio Heatmap (Bottom Center)
    ax8 = plt.subplot(3, 3, 8)
    
    # Calculate ratios for heatmap
    ratio_data = []
    metrics = ['qps', 'latency_50', 'latency_99', 'server_cpu_usage']
    metric_labels = ['QPS', 'P50 Latency', 'P99 Latency', 'CPU Usage']
    
    for scenario in scenarios:
        scenario_ratios = []
        inproc_data = df[(df['transport'] == 'inproc') & (df['scenario_base'] == scenario)]
        shmem_data = df[(df['transport'] == 'shmem') & (df['scenario_base'] == scenario)]
        
        if len(inproc_data) > 0 and len(shmem_data) > 0:
            for metric in metrics:
                # For latency and CPU, lower is better, so we invert the ratio
                if 'latency' in metric or 'cpu' in metric:
                    ratio = inproc_data[metric].iloc[0] / shmem_data[metric].iloc[0]
                else:
                    ratio = shmem_data[metric].iloc[0] / inproc_data[metric].iloc[0]
                scenario_ratios.append(ratio)
        else:
            scenario_ratios = [1.0] * len(metrics)
        
        ratio_data.append(scenario_ratios)
    
    ratio_array = np.array(ratio_data).T
    im = ax8.imshow(ratio_array, cmap='RdYlGn', aspect='auto', vmin=0.5, vmax=2.0)
    
    ax8.set_xticks(range(len(scenarios)))
    ax8.set_xticklabels(scenarios, rotation=45, ha='right')
    ax8.set_yticks(range(len(metric_labels)))
    ax8.set_yticklabels(metric_labels)
    ax8.set_title('Performance Ratio\n(Green = SHMEM Better)', fontweight='bold')
    
    # Add text annotations
    for i in range(len(metric_labels)):
        for j in range(len(scenarios)):
            text = ax8.text(j, i, f'{ratio_array[i, j]:.2f}', 
                           ha="center", va="center", color="black", fontweight='bold')
    
    plt.colorbar(im, ax=ax8, label='Ratio (shmem/inproc)')
    
    # 9. Summary Statistics (Bottom Right)
    ax9 = plt.subplot(3, 3, 9)
    ax9.axis('off')
    
    # Calculate summary statistics
    inproc_data = df[df['transport'] == 'inproc']
    shmem_data = df[df['transport'] == 'shmem']
    
    summary_text = f"""
PERFORMANCE SUMMARY

Average QPS:
• InProc: {inproc_data['qps'].mean():.1f}
• Shmem:  {shmem_data['qps'].mean():.1f}
• Ratio:  {shmem_data['qps'].mean() / inproc_data['qps'].mean():.3f}

Average P50 Latency:
• InProc: {inproc_data['latency_50'].mean():.1f} μs
• Shmem:  {shmem_data['latency_50'].mean():.1f} μs
• Ratio:  {shmem_data['latency_50'].mean() / inproc_data['latency_50'].mean():.3f}

Average CPU Usage:
• InProc: {inproc_data['server_cpu_usage'].mean():.2f}
• Shmem:  {shmem_data['server_cpu_usage'].mean():.2f}
• Ratio:  {shmem_data['server_cpu_usage'].mean() / inproc_data['server_cpu_usage'].mean():.3f}

Best Scenarios for Shmem:
"""
    
    # Find best scenarios for shmem
    best_scenarios = []
    for scenario in scenarios:
        inproc_qps = df[(df['transport'] == 'inproc') & (df['scenario_base'] == scenario)]['qps'].iloc[0]
        shmem_qps = df[(df['transport'] == 'shmem') & (df['scenario_base'] == scenario)]['qps'].iloc[0]
        if shmem_qps > inproc_qps:
            improvement = (shmem_qps / inproc_qps - 1) * 100
            best_scenarios.append(f"• {scenario}: +{improvement:.1f}%")
    
    if best_scenarios:
        summary_text += "\n" + "\n".join(best_scenarios)
    else:
        summary_text += "\n• None (InProc faster overall)"
    
    ax9.text(0.05, 0.95, summary_text, transform=ax9.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.5))
    
    plt.tight_layout()
    plt.savefig('comprehensive_transport_comparison.png', dpi=300, bbox_inches='tight')
    plt.savefig('comprehensive_transport_comparison.pdf', bbox_inches='tight')
    print("Comprehensive comparison plots saved to comprehensive_transport_comparison.png/pdf")

def create_detailed_analysis():
    """Create detailed analysis charts."""
    
    df = pd.read_csv('transport_benchmark_results.csv')
    df['scenario_base'] = df['scenario'].str.replace(r'^(inproc|shmem)_', '', regex=True)
    
    # Create figure for detailed analysis
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Detailed Transport Analysis', fontsize=16, fontweight='bold')
    
    # 1. QPS vs Latency Scatter Plot
    ax1 = axes[0, 0]
    for transport in ['inproc', 'shmem']:
        transport_data = df[df['transport'] == transport]
        ax1.scatter(transport_data['latency_50'], transport_data['qps'], 
                   label=transport, s=100, alpha=0.7)
        
        # Add scenario labels
        for _, row in transport_data.iterrows():
            ax1.annotate(row['scenario_base'], 
                        (row['latency_50'], row['qps']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8)
    
    ax1.set_xlabel('P50 Latency (μs)')
    ax1.set_ylabel('QPS')
    ax1.set_title('QPS vs Latency Trade-off')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Async vs Sync Performance
    ax2 = axes[0, 1]
    
    sync_data = df[df['scenario_base'].str.contains('sync')]
    async_data = df[df['scenario_base'].str.contains('async')]
    
    sync_summary = sync_data.groupby('transport')['qps'].mean()
    async_summary = async_data.groupby('transport')['qps'].mean()
    
    x = ['Sync', 'Async']
    inproc_vals = [sync_summary['inproc'], async_summary['inproc']]
    shmem_vals = [sync_summary['shmem'], async_summary['shmem']]
    
    x_pos = np.arange(len(x))
    width = 0.35
    
    ax2.bar(x_pos - width/2, inproc_vals, width, label='inproc', alpha=0.8)
    ax2.bar(x_pos + width/2, shmem_vals, width, label='shmem', alpha=0.8)
    
    ax2.set_xlabel('Client Type')
    ax2.set_ylabel('Average QPS')
    ax2.set_title('Sync vs Async Performance')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(x)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Channel Scaling Analysis
    ax3 = axes[1, 0]
    
    ch1_data = df[df['scenario_base'].str.contains('1ch')]
    ch4_data = df[df['scenario_base'].str.contains('4ch')]
    
    # Average performance for 1 vs 4 channels
    ch1_summary = ch1_data.groupby('transport')['qps'].mean()
    ch4_summary = ch4_data.groupby('transport')['qps'].mean()
    
    channels = ['1 Channel', '4 Channels']
    inproc_ch = [ch1_summary['inproc'], ch4_summary['inproc']]
    shmem_ch = [ch1_summary['shmem'], ch4_summary['shmem']]
    
    x_pos = np.arange(len(channels))
    ax3.bar(x_pos - width/2, inproc_ch, width, label='inproc', alpha=0.8)
    ax3.bar(x_pos + width/2, shmem_ch, width, label='shmem', alpha=0.8)
    
    ax3.set_xlabel('Channel Configuration')
    ax3.set_ylabel('Average QPS')
    ax3.set_title('Channel Scaling Performance')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels(channels)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Resource Efficiency
    ax4 = axes[1, 1]
    
    # Calculate efficiency metrics
    df['efficiency'] = df['qps'] / df['server_cpu_usage']
    df['latency_efficiency'] = 1000 / df['latency_50']  # Lower latency = higher efficiency
    
    for transport in ['inproc', 'shmem']:
        transport_data = df[df['transport'] == transport]
        ax4.scatter(transport_data['efficiency'], transport_data['latency_efficiency'], 
                   label=transport, s=100, alpha=0.7)
    
    ax4.set_xlabel('CPU Efficiency (QPS/CPU)')
    ax4.set_ylabel('Latency Efficiency (1000/μs)')
    ax4.set_title('Resource Efficiency Analysis')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('detailed_transport_analysis.png', dpi=300, bbox_inches='tight')
    print("Detailed analysis plots saved to detailed_transport_analysis.png")

def main():
    print("Loading and analyzing transport benchmark data...\n")
    
    # Load and analyze data
    df = load_and_analyze_data()
    
    # Create comprehensive plots
    print("Generating comprehensive comparison plots...")
    create_comprehensive_plots(df)
    
    # Create detailed analysis
    print("Generating detailed analysis plots...")
    create_detailed_analysis()
    
    print("\nAnalysis complete! Generated files:")
    print("  - comprehensive_transport_comparison.png/pdf")
    print("  - detailed_transport_analysis.png")
    print("  - transport_benchmark_results.csv")

if __name__ == "__main__":
    main()
