#!/usr/bin/env python3
"""
Comprehensive QPS benchmark comparing inproc, TCP, and shmem transports.
Collects performance metrics and generates comparison plots.
"""

import subprocess
import json
import csv
import re
import os
import sys
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

class TransportBenchmark:
    def __init__(self):
        self.results = []
        self.qps_binary = "./bazel-bin/test/cpp/qps/qps_json_driver"
        
    def create_scenario(self, name: str, client_type: str = "SYNC_CLIENT", 
                       server_type: str = "SYNC_SERVER", channels: int = 1,
                       outstanding_rpcs: int = 1, threads: int = 1) -> Dict:
        """Create a QPS scenario configuration."""
        return {
            "scenarios": [{
                "name": name,
                "client_config": {
                    "client_type": client_type,
                    "security_params": {"use_test_ca": True},
                    "outstanding_rpcs_per_channel": outstanding_rpcs,
                    "client_channels": channels,
                    "async_client_threads": threads,
                    "rpc_type": "UNARY",
                    "load_params": {"closed_loop": {}}
                },
                "server_config": {
                    "server_type": server_type,
                    "security_params": {"use_test_ca": True},
                    "async_server_threads": threads
                },
                "num_clients": 1,
                "num_servers": 1,
                "warmup_seconds": 3,
                "benchmark_seconds": 5
            }]
        }
    
    def run_benchmark(self, transport: str, scenario_config: Dict) -> Dict:
        """Run a single benchmark and parse results."""
        print(f"Running {transport} benchmark: {scenario_config['scenarios'][0]['name']}")
        
        if transport == "tcp":
            # TCP requires distributed mode
            return self.run_tcp_distributed(scenario_config)
        
        # Build command for inproc and shmem
        cmd = [self.qps_binary]
        
        if transport == "inproc":
            cmd.extend(["--run_inproc"])
        elif transport == "shmem":
            cmd.extend(["--run_inproc", "--inproc_transport=shmem"])
        
        cmd.extend([f"--scenarios_json={json.dumps(scenario_config)}"])
        
        try:
            # Run benchmark
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode != 0:
                print(f"Error running {transport} benchmark:")
                print(result.stderr)
                return None
                
            # Parse output
            metrics = self.parse_output(result.stderr, transport, scenario_config['scenarios'][0]['name'])
            return metrics
            
        except subprocess.TimeoutExpired:
            print(f"Timeout running {transport} benchmark")
            return None
        except Exception as e:
            print(f"Exception running {transport} benchmark: {e}")
            return None
    
    def run_tcp_distributed(self, scenario_config: Dict) -> Dict:
        """Run TCP benchmark using distributed mode script"""
        try:
            # Use the distributed TCP script
            scenario_str = json.dumps(scenario_config)
            
            # Run using the distributed TCP script
            cmd = [
                './run_qps_tcp_distributed.sh',
                '--clients', '1',
                '--base_port', '20000',
                '--scenarios_json', scenario_str
            ]
            
            scenario_name = scenario_config['scenarios'][0]['name']
            print(f"Running TCP benchmark for {scenario_name} using distributed mode...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,  # 3 minute timeout
                cwd='/datadrive/grpc'
            )
            
            if result.returncode != 0:
                print(f"TCP benchmark failed for {scenario_name}")
                print(f"STDERR: {result.stderr}")
                print(f"STDOUT: {result.stdout}")
                return None
            
            # Parse the output (should be in stderr like other QPS outputs)
            output_to_parse = result.stderr if result.stderr.strip() else result.stdout
            return self.parse_output(output_to_parse, "tcp", scenario_name)
            
        except subprocess.TimeoutExpired:
            print(f"TCP benchmark timed out for {scenario_name}")
            return None
        except Exception as e:
            print(f"TCP benchmark failed for {scenario_name}: {e}")
            return None
    
    def parse_output(self, output: str, transport: str, scenario: str) -> Dict:
        """Parse QPS benchmark output to extract metrics."""
        metrics = {
            'transport': transport,
            'scenario': scenario,
            'qps': 0.0,
            'qps_per_core': 0.0,
            'latency_50': 0.0,
            'latency_90': 0.0,
            'latency_95': 0.0,
            'latency_99': 0.0,
            'latency_999': 0.0,
            'server_system_time': 0.0,
            'server_user_time': 0.0,
            'client_system_time': 0.0,
            'client_user_time': 0.0,
            'server_cpu_usage': 0.0,
            'client_polls_per_request': 0.0,
            'server_polls_per_request': 0.0,
            'server_queries_per_cpu_sec': 0.0,
            'client_queries_per_cpu_sec': 0.0
        }
        
        # Parse QPS
        qps_match = re.search(r'QPS: ([\d.]+)', output)
        if qps_match:
            metrics['qps'] = float(qps_match.group(1))
            
        # Parse QPS per core
        qps_core_match = re.search(r'QPS: [\d.]+ \(([\d.]+)/server core\)', output)
        if qps_core_match:
            metrics['qps_per_core'] = float(qps_core_match.group(1))
            
        # Parse latencies
        latency_match = re.search(r'Latencies \(50/90/95/99/99\.9%-ile\): ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) us', output)
        if latency_match:
            metrics['latency_50'] = float(latency_match.group(1))
            metrics['latency_90'] = float(latency_match.group(2))
            metrics['latency_95'] = float(latency_match.group(3))
            metrics['latency_99'] = float(latency_match.group(4))
            metrics['latency_999'] = float(latency_match.group(5))
            
        # Parse system metrics
        server_sys_match = re.search(r'Server system time: ([\d.]+)', output)
        if server_sys_match:
            metrics['server_system_time'] = float(server_sys_match.group(1))
            
        server_user_match = re.search(r'Server user time:\s+([\d.]+)', output)
        if server_user_match:
            metrics['server_user_time'] = float(server_user_match.group(1))
            
        client_sys_match = re.search(r'Client system time: ([\d.]+)', output)
        if client_sys_match:
            metrics['client_system_time'] = float(client_sys_match.group(1))
            
        client_user_match = re.search(r'Client user time:\s+([\d.]+)', output)
        if client_user_match:
            metrics['client_user_time'] = float(client_user_match.group(1))
            
        # Parse CPU usage
        server_cpu_match = re.search(r'Server CPU usage: ([\d.]+)', output)
        if server_cpu_match:
            metrics['server_cpu_usage'] = float(server_cpu_match.group(1))
            
        # Parse polls per request
        client_polls_match = re.search(r'Client Polls per Request: ([\d.]+)', output)
        if client_polls_match:
            metrics['client_polls_per_request'] = float(client_polls_match.group(1))
            
        server_polls_match = re.search(r'Server Polls per Request: ([\d.]+)', output)
        if server_polls_match:
            metrics['server_polls_per_request'] = float(server_polls_match.group(1))
            
        # Parse queries per CPU-sec
        server_queries_match = re.search(r'Server Queries/CPU-sec: ([\d.]+)', output)
        if server_queries_match:
            metrics['server_queries_per_cpu_sec'] = float(server_queries_match.group(1))
            
        client_queries_match = re.search(r'Client Queries/CPU-sec: ([\d.]+)', output)
        if client_queries_match:
            metrics['client_queries_per_cpu_sec'] = float(client_queries_match.group(1))
            
        return metrics
    
    def run_all_benchmarks(self):
        """Run comprehensive benchmark suite."""
        transports = ["inproc", "shmem", "tcp"]
        
        # Test scenarios with different configurations
        scenarios = [
            ("sync_1ch_1rpc", "SYNC_CLIENT", "SYNC_SERVER", 1, 1, 1),
            ("sync_1ch_10rpc", "SYNC_CLIENT", "SYNC_SERVER", 1, 10, 1),
            ("sync_4ch_1rpc", "SYNC_CLIENT", "SYNC_SERVER", 4, 1, 1),
            ("async_1ch_1rpc", "ASYNC_CLIENT", "ASYNC_SERVER", 1, 1, 1),
            ("async_1ch_10rpc", "ASYNC_CLIENT", "ASYNC_SERVER", 1, 10, 1),
            ("async_4ch_1rpc", "ASYNC_CLIENT", "ASYNC_SERVER", 4, 1, 1),
        ]
        
        for transport in transports:
            print(f"\n=== Running {transport.upper()} Transport Benchmarks ===")
            
            for scenario_name, client_type, server_type, channels, rpcs, threads in scenarios:
                scenario_config = self.create_scenario(
                    f"{transport}_{scenario_name}", client_type, server_type, 
                    channels, rpcs, threads
                )
                
                metrics = self.run_benchmark(transport, scenario_config)
                if metrics:
                    self.results.append(metrics)
                    print(f"  {scenario_name}: {metrics['qps']:.2f} QPS, {metrics['latency_50']:.2f}μs p50")
                else:
                    print(f"  {scenario_name}: FAILED")
    
    def save_to_csv(self, filename: str = "transport_benchmark_results.csv"):
        """Save results to CSV file."""
        if not self.results:
            print("No results to save")
            return
            
        df = pd.DataFrame(self.results)
        df.to_csv(filename, index=False)
        print(f"Results saved to {filename}")
        
    def generate_plots(self):
        """Generate comparison plots."""
        if not self.results:
            print("No results to plot")
            return
            
        df = pd.DataFrame(self.results)
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('gRPC Transport Performance Comparison', fontsize=16, fontweight='bold')
        
        # Extract scenario names for grouping
        df['scenario_base'] = df['scenario'].str.replace(r'^(inproc|shmem|tcp)_', '', regex=True)
        
        # 1. QPS Comparison
        ax1 = axes[0, 0]
        scenarios = df['scenario_base'].unique()
        x = np.arange(len(scenarios))
        width = 0.25
        
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
            transport_data = df[df['transport'] == transport]
            qps_values = [transport_data[transport_data['scenario_base'] == s]['qps'].iloc[0] 
                         if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                         for s in scenarios]
            ax1.bar(x + i*width, qps_values, width, label=transport)
        
        ax1.set_xlabel('Scenario')
        ax1.set_ylabel('QPS')
        ax1.set_title('Queries Per Second (QPS)')
        ax1.set_xticks(x + width)
        ax1.set_xticklabels(scenarios, rotation=45, ha='right')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Latency P50 Comparison
        ax2 = axes[0, 1]
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
            transport_data = df[df['transport'] == transport]
            lat_values = [transport_data[transport_data['scenario_base'] == s]['latency_50'].iloc[0] 
                         if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                         for s in scenarios]
            ax2.bar(x + i*width, lat_values, width, label=transport)
        
        ax2.set_xlabel('Scenario')
        ax2.set_ylabel('Latency (μs)')
        ax2.set_title('Median Latency (P50)')
        ax2.set_xticks(x + width)
        ax2.set_xticklabels(scenarios, rotation=45, ha='right')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. Latency P99 Comparison
        ax3 = axes[0, 2]
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
            transport_data = df[df['transport'] == transport]
            lat_values = [transport_data[transport_data['scenario_base'] == s]['latency_99'].iloc[0] 
                         if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                         for s in scenarios]
            ax3.bar(x + i*width, lat_values, width, label=transport)
        
        ax3.set_xlabel('Scenario')
        ax3.set_ylabel('Latency (μs)')
        ax3.set_title('P99 Latency')
        ax3.set_xticks(x + width)
        ax3.set_xticklabels(scenarios, rotation=45, ha='right')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 4. CPU Usage Comparison
        ax4 = axes[1, 0]
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
            transport_data = df[df['transport'] == transport]
            cpu_values = [transport_data[transport_data['scenario_base'] == s]['server_cpu_usage'].iloc[0] 
                         if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                         for s in scenarios]
            ax4.bar(x + i*width, cpu_values, width, label=transport)
        
        ax4.set_xlabel('Scenario')
        ax4.set_ylabel('CPU Usage')
        ax4.set_title('Server CPU Usage')
        ax4.set_xticks(x + width)
        ax4.set_xticklabels(scenarios, rotation=45, ha='right')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        # 5. QPS per Core Comparison
        ax5 = axes[1, 1]
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
            transport_data = df[df['transport'] == transport]
            qps_core_values = [transport_data[transport_data['scenario_base'] == s]['qps_per_core'].iloc[0] 
                              if len(transport_data[transport_data['scenario_base'] == s]) > 0 else 0 
                              for s in scenarios]
            ax5.bar(x + i*width, qps_core_values, width, label=transport)
        
        ax5.set_xlabel('Scenario')
        ax5.set_ylabel('QPS per Core')
        ax5.set_title('QPS per Server Core')
        ax5.set_xticks(x + width)
        ax5.set_xticklabels(scenarios, rotation=45, ha='right')
        ax5.legend()
        ax5.grid(True, alpha=0.3)
        
        # 6. Efficiency Comparison (QPS per CPU usage)
        ax6 = axes[1, 2]
        for i, transport in enumerate(['inproc', 'shmem', 'tcp']):
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
            ax6.bar(x + i*width, efficiency_values, width, label=transport)
        
        ax6.set_xlabel('Scenario')
        ax6.set_ylabel('QPS / CPU Usage')
        ax6.set_title('Efficiency (QPS per CPU Usage)')
        ax6.set_xticks(x + width)
        ax6.set_xticklabels(scenarios, rotation=45, ha='right')
        ax6.legend()
        ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('transport_performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.savefig('transport_performance_comparison.pdf', bbox_inches='tight')
        print("Performance comparison plots saved to transport_performance_comparison.png/pdf")
        
        # Generate latency distribution plot
        self.plot_latency_distribution(df)
        
    def plot_latency_distribution(self, df):
        """Generate latency distribution comparison plot."""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Select a representative scenario for latency distribution
        test_scenario = 'sync_1ch_1rpc'
        scenario_data = df[df['scenario_base'] == test_scenario]
        
        percentiles = ['50', '90', '95', '99', '999']
        percentile_labels = ['P50', 'P90', 'P95', 'P99', 'P99.9']
        
        for transport in ['inproc', 'shmem', 'tcp']:
            transport_data = scenario_data[scenario_data['transport'] == transport]
            if len(transport_data) > 0:
                latencies = [
                    transport_data[f'latency_{p}'].iloc[0] for p in percentiles
                ]
                ax.plot(percentile_labels, latencies, marker='o', linewidth=2, label=transport)
        
        ax.set_xlabel('Latency Percentile')
        ax.set_ylabel('Latency (μs)')
        ax.set_title(f'Latency Distribution Comparison - {test_scenario}')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_yscale('log')
        
        plt.tight_layout()
        plt.savefig('latency_distribution_comparison.png', dpi=300, bbox_inches='tight')
        print("Latency distribution plot saved to latency_distribution_comparison.png")

def main():
    # Check if QPS binary exists
    qps_binary = "./bazel-bin/test/cpp/qps/qps_json_driver"
    if not os.path.exists(qps_binary):
        print(f"QPS binary not found at {qps_binary}")
        print("Please ensure gRPC QPS tools are built with: bazel build test/cpp/qps:qps_json_driver")
        sys.exit(1)
    
    benchmark = TransportBenchmark()
    
    print("Starting comprehensive transport benchmark...")
    print("This will test inproc, shmem, and TCP transports across multiple scenarios")
    print("Estimated time: 5-10 minutes\n")
    
    benchmark.run_all_benchmarks()
    benchmark.save_to_csv()
    benchmark.generate_plots()
    
    print("\nBenchmark complete!")
    print("Results saved to:")
    print("  - transport_benchmark_results.csv")
    print("  - transport_performance_comparison.png/pdf")
    print("  - latency_distribution_comparison.png")

if __name__ == "__main__":
    main()
