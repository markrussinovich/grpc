#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re

def parse_benchmark_name(name):
    """Parse benchmark name to extract transport, size, and other parameters"""
    # Extract transport type
    transport_match = re.search(r'<([^,]+),', name)
    transport = transport_match.group(1) if transport_match else 'Unknown'
    
    # Map transport names to match unary style
    transport_mapping = {
        'TCP': 'TCP',
        'InProcess': 'InProcess',
        'ShmemTransport': 'ShmemTransport',
        'MinTCP': 'MinTCP',
        'MinInProcess': 'MinInProcess',
        'MinShmemTransport': 'MinShmemTransport'
    }
    
    transport = transport_mapping.get(transport, transport)
    
    # Extract size parameters
    size_params = re.findall(r'/(\d+)', name)
    
    # For StreamingPingPong, get the first size parameter
    if 'StreamingPingPong<' in name and 'StreamingPingPongMsgs' not in name and 'StreamingPingPongWithCoalescingApi' not in name:
        size = int(size_params[0]) if size_params else 0
        return transport, size
    
    return None, None

def load_and_process_data(csv_file):
    """Load and process the benchmark data"""
    # Skip the header lines and start from the CSV header
    df = pd.read_csv(csv_file, skiprows=9)
    
    # Parse benchmark names
    parsed_data = []
    for _, row in df.iterrows():
        transport, size = parse_benchmark_name(row['name'])
        if transport is not None:
            parsed_data.append({
                'transport': transport,
                'size': size,
                'real_time': row['real_time'],  # in nanoseconds
                'cpu_time': row['cpu_time'],
                'bytes_per_second': row['bytes_per_second'] if row['bytes_per_second'] > 0 else np.nan
            })
    
    return pd.DataFrame(parsed_data)

def create_unified_transport_plot():
    """Create unified transport performance plot similar to unary_transport_performance.png"""
    # Load data
    df = load_and_process_data('stream_bench_results.csv')
    
    # Convert nanoseconds to microseconds for latency
    df['latency_us'] = df['real_time'] / 1000
    
    # Define colors similar to the unary plot (excluding Min versions)
    colors = {
        'InProcess': '#2E8B57',      # Sea Green
        'ShmemTransport': '#FF6347',  # Tomato
        'TCP': '#9370DB',            # Medium Purple
    }
    
    # Filter out Min versions
    df = df[~df['transport'].str.startswith('Min')]
    
    # Create the figure with 2x3 subplots
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Streaming Transport Performance Comparison', fontsize=16, fontweight='bold')
    
    # Plot 1: 0-Payload RPC Latency (size=0)
    ax1 = axes[0, 0]
    zero_payload = df[df['size'] == 0].groupby('transport')['latency_us'].mean().sort_values()
    
    bars = ax1.bar(range(len(zero_payload)), zero_payload.values, 
                   color=[colors.get(t, '#888888') for t in zero_payload.index])
    ax1.set_title('0-Payload Streaming Latency', fontweight='bold')
    ax1.set_ylabel('Latency (microseconds)')
    ax1.set_xticks(range(len(zero_payload)))
    ax1.set_xticklabels(zero_payload.index, rotation=45, ha='right')
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, zero_payload.values)):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + bar.get_height()*0.01,
                f'{val:.0f}μs', ha='center', va='bottom', fontweight='bold')
    
    # Plot 2: Streaming Latency vs Message Size (Log-Log Scale)
    ax2 = axes[0, 1]
    for transport in df['transport'].unique():
        if transport in colors:
            transport_data = df[df['transport'] == transport]
            transport_data = transport_data[transport_data['size'] > 0]  # Remove size 0 for log scale
            if not transport_data.empty:
                grouped = transport_data.groupby('size')['latency_us'].mean().sort_index()
                ax2.loglog(grouped.index, grouped.values, 'o-', 
                          label=transport, color=colors[transport], linewidth=2, markersize=6)
    
    ax2.set_title('Streaming Latency vs Message Size (Log-Log Scale)', fontweight='bold')
    ax2.set_xlabel('Message Size (bytes)')
    ax2.set_ylabel('Latency (microseconds)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Throughput vs Message Size (Log-Log Scale)
    ax3 = axes[0, 2]
    for transport in df['transport'].unique():
        if transport in colors:
            transport_data = df[df['transport'] == transport]
            transport_data = transport_data[transport_data['size'] > 0]  # Remove size 0 for log scale
            # Calculate throughput in GB/s
            transport_data = transport_data.dropna(subset=['bytes_per_second'])
            if not transport_data.empty:
                transport_data['throughput_gbps'] = transport_data['bytes_per_second'] / (1024**3)
                grouped = transport_data.groupby('size')['throughput_gbps'].mean().sort_index()
                if not grouped.empty:
                    ax3.loglog(grouped.index, grouped.values, 'o-', 
                              label=transport, color=colors[transport], linewidth=2, markersize=6)
    
    ax3.set_title('Streaming Throughput vs Message Size (Log-Log Scale)', fontweight='bold')
    ax3.set_xlabel('Message Size (bytes)')
    ax3.set_ylabel('Throughput (GB/s)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Small Message Performance (Linear Scale, 0-4KB)
    ax4 = axes[1, 0]
    small_msg_sizes = [1, 8, 64, 512, 4096]
    for transport in df['transport'].unique():
        if transport in colors:
            transport_data = df[df['transport'] == transport]
            latencies = []
            sizes_available = []
            for size in small_msg_sizes:
                size_data = transport_data[transport_data['size'] == size]
                if not size_data.empty:
                    latencies.append(size_data['latency_us'].mean())
                    sizes_available.append(size)
            
            if latencies:
                ax4.plot(sizes_available, latencies, 'o-', 
                        label=transport, color=colors[transport], linewidth=2, markersize=6)
    
    ax4.set_title('Small Message Performance (Linear Scale)', fontweight='bold')
    ax4.set_xlabel('Message Size (bytes)')
    ax4.set_ylabel('Latency (microseconds)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # Plot 5: Medium Message Performance (4KB - 256KB)
    ax5 = axes[1, 1]
    medium_msg_sizes = [4096, 32768, 262144]
    for transport in df['transport'].unique():
        if transport in colors:
            transport_data = df[df['transport'] == transport]
            latencies = []
            sizes_available = []
            for size in medium_msg_sizes:
                size_data = transport_data[transport_data['size'] == size]
                if not size_data.empty:
                    latencies.append(size_data['latency_us'].mean())
                    sizes_available.append(size)
            
            if latencies:
                ax5.plot(sizes_available, latencies, 'o-', 
                        label=transport, color=colors[transport], linewidth=2, markersize=6)
    
    ax5.set_title('Medium Message Performance (4KB - 256KB)', fontweight='bold')
    ax5.set_xlabel('Message Size (bytes)')
    ax5.set_ylabel('Latency (microseconds)')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    
    # Plot 6: Large Message Performance (line plot with log scale for better visibility)
    ax6 = axes[1, 2]
    large_msg_sizes = [2097152, 16777216, 134217728]  # 2MB, 16MB, 128MB
    
    for transport in ['InProcess', 'ShmemTransport', 'TCP']:
        if transport in df['transport'].unique():
            transport_data = df[df['transport'] == transport]
            latencies = []
            sizes_available = []
            for size in large_msg_sizes:
                size_data = transport_data[transport_data['size'] == size]
                if not size_data.empty:
                    # Keep in microseconds for better resolution
                    latencies.append(size_data['latency_us'].mean())
                    sizes_available.append(size)
            
            if latencies:
                ax6.loglog(sizes_available, latencies, 'o-', 
                          label=transport, color=colors.get(transport, '#888888'),
                          linewidth=3, markersize=10)
                
                # Add value labels
                for x, y in zip(sizes_available, latencies):
                    if y < 1000:
                        label = f'{y:.0f}μs'
                    else:
                        label = f'{y/1000:.1f}ms'
                    ax6.annotate(label, (x, y), 
                               xytext=(5, 10), textcoords='offset points',
                               fontsize=9, ha='left')
    
    ax6.set_title('Large Message Performance (Log Scale)', fontweight='bold')
    ax6.set_xlabel('Message Size (bytes)')
    ax6.set_ylabel('Latency (microseconds)')
    ax6.legend()
    ax6.grid(True, alpha=0.3)
    
    # Set custom x-tick labels
    ax6.set_xticks(large_msg_sizes)
    ax6.set_xticklabels(['2MB', '16MB', '128MB'])
    
    # Adjust layout
    plt.tight_layout()
    
    return fig

def main():
    """Main function to generate the unified plot"""
    print("Creating unified streaming transport performance plot...")
    
    fig = create_unified_transport_plot()
    
    # Save the plot
    fig.savefig('streaming_transport_performance.png', dpi=300, bbox_inches='tight')
    fig.savefig('streaming_transport_performance.pdf', bbox_inches='tight')
    
    print("Plot saved as:")
    print("- streaming_transport_performance.png")
    print("- streaming_transport_performance.pdf")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    main()