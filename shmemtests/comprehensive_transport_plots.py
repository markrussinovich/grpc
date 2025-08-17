#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.patches import Rectangle
import re

# Set style for better plots
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def parse_benchmark_name(name):
    """Parse benchmark name to extract transport, size, and other parameters"""
    # Extract transport type
    transport_match = re.search(r'<([^,]+),', name)
    transport = transport_match.group(1) if transport_match else 'Unknown'
    
    # Extract size parameters
    size_params = re.findall(r'/(\d+)', name)
    
    # Extract benchmark type
    if 'StreamingPingPongMsgs' in name:
        bench_type = 'StreamingPingPongMsgs'
        size = int(size_params[0]) if size_params else 0
        return transport, bench_type, size, None, None
    elif 'StreamingPingPongWithCoalescingApi' in name:
        bench_type = 'StreamingPingPongWithCoalescingApi'
        size = int(size_params[0]) if len(size_params) >= 1 else 0
        param1 = int(size_params[1]) if len(size_params) >= 2 else None
        param2 = int(size_params[2]) if len(size_params) >= 3 else None
        return transport, bench_type, size, param1, param2
    elif 'StreamingPingPong' in name:
        bench_type = 'StreamingPingPong'
        size = int(size_params[0]) if len(size_params) >= 1 else 0
        param1 = int(size_params[1]) if len(size_params) >= 2 else None
        return transport, bench_type, size, param1, None
    
    return transport, 'Unknown', 0, None, None

def load_and_process_data(csv_file):
    """Load and process the benchmark data"""
    # Skip the header lines and start from the CSV header
    df = pd.read_csv(csv_file, skiprows=9)
    
    # Parse benchmark names
    parsed_data = []
    for _, row in df.iterrows():
        transport, bench_type, size, param1, param2 = parse_benchmark_name(row['name'])
        parsed_data.append({
            'transport': transport,
            'benchmark_type': bench_type,
            'size': size,
            'param1': param1,
            'param2': param2,
            'real_time': row['real_time'],
            'cpu_time': row['cpu_time'],
            'bytes_per_second': row['bytes_per_second'] if row['bytes_per_second'] > 0 else np.nan,
            'iterations': row['iterations']
        })
    
    return pd.DataFrame(parsed_data)

def create_throughput_comparison(df):
    """Create throughput comparison plots across transports and sizes"""
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    
    # Filter data for main benchmark types
    ping_pong_df = df[df['benchmark_type'] == 'StreamingPingPong'].copy()
    ping_pong_msgs_df = df[df['benchmark_type'] == 'StreamingPingPongMsgs'].copy()
    
    # Convert bytes_per_second to GB/s for better readability
    ping_pong_df['throughput_gbps'] = ping_pong_df['bytes_per_second'] / (1024**3)
    ping_pong_msgs_df['throughput_gbps'] = ping_pong_msgs_df['bytes_per_second'] / (1024**3)
    
    # Plot 1: StreamingPingPong throughput vs size
    ax1 = axes[0, 0]
    for transport in ping_pong_df['transport'].unique():
        if pd.isna(transport) or transport == 'Unknown':
            continue
        transport_data = ping_pong_df[ping_pong_df['transport'] == transport]
        if not transport_data.empty:
            # Group by size and take mean for multiple param configurations
            grouped = transport_data.groupby('size')['throughput_gbps'].mean().reset_index()
            grouped = grouped[grouped['size'] > 0]  # Remove size 0
            if not grouped.empty:
                ax1.loglog(grouped['size'], grouped['throughput_gbps'], 'o-', 
                          label=transport, linewidth=2, markersize=8)
    
    ax1.set_xlabel('Message Size (bytes)', fontsize=12)
    ax1.set_ylabel('Throughput (GB/s)', fontsize=12)
    ax1.set_title('StreamingPingPong: Throughput vs Message Size', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: StreamingPingPongMsgs throughput vs size
    ax2 = axes[0, 1]
    for transport in ping_pong_msgs_df['transport'].unique():
        if pd.isna(transport) or transport == 'Unknown':
            continue
        transport_data = ping_pong_msgs_df[ping_pong_msgs_df['transport'] == transport]
        if not transport_data.empty:
            transport_data = transport_data[transport_data['size'] > 0]  # Remove size 0
            if not transport_data.empty:
                ax2.loglog(transport_data['size'], transport_data['throughput_gbps'], 'o-', 
                          label=transport, linewidth=2, markersize=8)
    
    ax2.set_xlabel('Message Size (bytes)', fontsize=12)
    ax2.set_ylabel('Throughput (GB/s)', fontsize=12)
    ax2.set_title('StreamingPingPongMsgs: Throughput vs Message Size', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Latency comparison (real_time)
    ax3 = axes[1, 0]
    for transport in ping_pong_df['transport'].unique():
        if pd.isna(transport) or transport == 'Unknown':
            continue
        transport_data = ping_pong_df[ping_pong_df['transport'] == transport]
        if not transport_data.empty:
            grouped = transport_data.groupby('size')['real_time'].mean().reset_index()
            grouped = grouped[grouped['size'] > 0]  # Remove size 0
            if not grouped.empty:
                # Convert nanoseconds to microseconds
                grouped['latency_us'] = grouped['real_time'] / 1000
                ax3.loglog(grouped['size'], grouped['latency_us'], 'o-', 
                          label=transport, linewidth=2, markersize=8)
    
    ax3.set_xlabel('Message Size (bytes)', fontsize=12)
    ax3.set_ylabel('Latency (μs)', fontsize=12)
    ax3.set_title('StreamingPingPong: Latency vs Message Size', fontsize=14, fontweight='bold')
    ax3.legend(fontsize=10)
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Efficiency comparison (throughput per unit latency)
    ax4 = axes[1, 1]
    for transport in ping_pong_df['transport'].unique():
        if pd.isna(transport) or transport == 'Unknown':
            continue
        transport_data = ping_pong_df[ping_pong_df['transport'] == transport]
        if not transport_data.empty:
            grouped = transport_data.groupby('size').agg({
                'throughput_gbps': 'mean',
                'real_time': 'mean'
            }).reset_index()
            grouped = grouped[grouped['size'] > 0]  # Remove size 0
            if not grouped.empty:
                # Calculate efficiency as throughput / latency (GB/s per μs)
                grouped['efficiency'] = grouped['throughput_gbps'] / (grouped['real_time'] / 1000)
                ax4.loglog(grouped['size'], grouped['efficiency'], 'o-', 
                          label=transport, linewidth=2, markersize=8)
    
    ax4.set_xlabel('Message Size (bytes)', fontsize=12)
    ax4.set_ylabel('Efficiency (GB/s per μs)', fontsize=12)
    ax4.set_title('Transport Efficiency: Throughput per Unit Latency', fontsize=14, fontweight='bold')
    ax4.legend(fontsize=10)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def create_performance_heatmap(df):
    """Create performance heatmaps for different metrics"""
    fig, axes = plt.subplots(1, 2, figsize=(20, 8))
    
    # Prepare data for heatmap
    ping_pong_df = df[df['benchmark_type'] == 'StreamingPingPong'].copy()
    ping_pong_df = ping_pong_df[ping_pong_df['size'] > 0]  # Remove size 0
    
    # Group by transport and size, take mean
    heatmap_data = ping_pong_df.groupby(['transport', 'size']).agg({
        'bytes_per_second': 'mean',
        'real_time': 'mean'
    }).reset_index()
    
    # Create pivot tables for heatmaps
    throughput_pivot = heatmap_data.pivot(index='transport', columns='size', values='bytes_per_second')
    latency_pivot = heatmap_data.pivot(index='transport', columns='size', values='real_time')
    
    # Convert to GB/s and μs
    throughput_pivot = throughput_pivot / (1024**3)
    latency_pivot = latency_pivot / 1000
    
    # Heatmap 1: Throughput
    sns.heatmap(throughput_pivot, annot=True, fmt='.2f', cmap='viridis', 
                ax=axes[0], cbar_kws={'label': 'Throughput (GB/s)'})
    axes[0].set_title('Throughput Heatmap (GB/s)', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Message Size (bytes)', fontsize=12)
    axes[0].set_ylabel('Transport', fontsize=12)
    
    # Heatmap 2: Latency
    sns.heatmap(latency_pivot, annot=True, fmt='.1f', cmap='plasma_r', 
                ax=axes[1], cbar_kws={'label': 'Latency (μs)'})
    axes[1].set_title('Latency Heatmap (μs)', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Message Size (bytes)', fontsize=12)
    axes[1].set_ylabel('Transport', fontsize=12)
    
    plt.tight_layout()
    return fig

def create_transport_summary(df):
    """Create summary statistics and bar charts"""
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    
    ping_pong_df = df[df['benchmark_type'] == 'StreamingPingPong'].copy()
    ping_pong_df = ping_pong_df[ping_pong_df['size'] > 0]
    
    # Convert units
    ping_pong_df['throughput_gbps'] = ping_pong_df['bytes_per_second'] / (1024**3)
    ping_pong_df['latency_us'] = ping_pong_df['real_time'] / 1000
    
    # Summary by transport
    transport_summary = ping_pong_df.groupby('transport').agg({
        'throughput_gbps': ['mean', 'max'],
        'latency_us': ['mean', 'min'],
        'size': 'count'
    }).round(3)
    
    # Flatten column names
    transport_summary.columns = ['_'.join(col).strip() for col in transport_summary.columns]
    transport_summary = transport_summary.reset_index()
    
    # Plot 1: Average throughput by transport
    ax1 = axes[0, 0]
    bars1 = ax1.bar(transport_summary['transport'], transport_summary['throughput_gbps_mean'])
    ax1.set_title('Average Throughput by Transport', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Average Throughput (GB/s)', fontsize=12)
    ax1.set_xlabel('Transport', fontsize=12)
    ax1.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar in bars1:
        height = bar.get_height()
        ax1.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    # Plot 2: Maximum throughput by transport
    ax2 = axes[0, 1]
    bars2 = ax2.bar(transport_summary['transport'], transport_summary['throughput_gbps_max'])
    ax2.set_title('Maximum Throughput by Transport', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Maximum Throughput (GB/s)', fontsize=12)
    ax2.set_xlabel('Transport', fontsize=12)
    ax2.tick_params(axis='x', rotation=45)
    
    for bar in bars2:
        height = bar.get_height()
        ax2.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    # Plot 3: Average latency by transport
    ax3 = axes[1, 0]
    bars3 = ax3.bar(transport_summary['transport'], transport_summary['latency_us_mean'])
    ax3.set_title('Average Latency by Transport', fontsize=14, fontweight='bold')
    ax3.set_ylabel('Average Latency (μs)', fontsize=12)
    ax3.set_xlabel('Transport', fontsize=12)
    ax3.tick_params(axis='x', rotation=45)
    
    for bar in bars3:
        height = bar.get_height()
        ax3.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    # Plot 4: Minimum latency by transport
    ax4 = axes[1, 1]
    bars4 = ax4.bar(transport_summary['transport'], transport_summary['latency_us_min'])
    ax4.set_title('Minimum Latency by Transport', fontsize=14, fontweight='bold')
    ax4.set_ylabel('Minimum Latency (μs)', fontsize=12)
    ax4.set_xlabel('Transport', fontsize=12)
    ax4.tick_params(axis='x', rotation=45)
    
    for bar in bars4:
        height = bar.get_height()
        ax4.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    plt.tight_layout()
    return fig, transport_summary

def main():
    """Main function to generate all plots"""
    print("Loading and processing benchmark data...")
    df = load_and_process_data('stream_bench_results.csv')
    
    print(f"Loaded {len(df)} benchmark results")
    print(f"Transports found: {df['transport'].unique()}")
    print(f"Benchmark types: {df['benchmark_type'].unique()}")
    
    # Create comprehensive performance comparison
    print("Creating throughput and latency comparison plots...")
    fig1 = create_throughput_comparison(df)
    fig1.savefig('comprehensive_transport_comparison.png', dpi=300, bbox_inches='tight')
    fig1.savefig('comprehensive_transport_comparison.pdf', bbox_inches='tight')
    
    # Create performance heatmaps
    print("Creating performance heatmaps...")
    fig2 = create_performance_heatmap(df)
    fig2.savefig('transport_performance_heatmaps.png', dpi=300, bbox_inches='tight')
    
    # Create summary statistics
    print("Creating transport summary...")
    fig3, summary_df = create_transport_summary(df)
    fig3.savefig('transport_summary_charts.png', dpi=300, bbox_inches='tight')
    
    # Save summary statistics to CSV
    summary_df.to_csv('transport_performance_summary.csv', index=False)
    
    print("\nTransport Performance Summary:")
    print(summary_df.to_string(index=False))
    
    print("\nPlots saved:")
    print("- comprehensive_transport_comparison.png")
    print("- comprehensive_transport_comparison.pdf")
    print("- transport_performance_heatmaps.png")
    print("- transport_summary_charts.png")
    print("- transport_performance_summary.csv")
    
    # Show plots
    plt.show()

if __name__ == "__main__":
    main()