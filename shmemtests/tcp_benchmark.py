#!/usr/bin/env python3
"""
Simple TCP benchmark using single-process client/server approach.
"""

import subprocess
import json
import time
import signal
import os
import sys
from typing import Dict

def run_tcp_benchmark():
    """Run TCP benchmark using separate server and client processes."""
    
    # Test scenario
    scenario = {
        "scenarios": [{
            "name": "tcp_sync_1ch_1rpc",
            "client_config": {
                "client_type": "SYNC_CLIENT",
                "security_params": {"use_test_ca": False},
                "outstanding_rpcs_per_channel": 1,
                "client_channels": 1,
                "async_client_threads": 1,
                "rpc_type": "UNARY",
                "load_params": {"closed_loop": {}}
            },
            "server_config": {
                "server_type": "SYNC_SERVER",
                "security_params": {"use_test_ca": False},
                "async_server_threads": 1
            },
            "num_clients": 1,
            "num_servers": 1,
            "warmup_seconds": 2,
            "benchmark_seconds": 5
        }]
    }
    
    try:
        # Start QPS worker as server
        print("Starting QPS server...")
        server_proc = subprocess.Popen([
            "./bazel-bin/test/cpp/qps/qps_worker",
            "--driver_port=12345"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Give server time to start
        time.sleep(2)
        
        # Run client with server address
        print("Running TCP benchmark...")
        os.environ["QPS_WORKERS"] = "localhost:12345"
        
        client_proc = subprocess.run([
            "./bazel-bin/test/cpp/qps/qps_json_driver",
            f"--scenarios_json={json.dumps(scenario)}"
        ], capture_output=True, text=True, timeout=30)
        
        # Clean up server
        server_proc.terminate()
        server_proc.wait(timeout=5)
        
        if client_proc.returncode == 0:
            print("TCP benchmark completed successfully")
            # Parse results similar to other transports
            output = client_proc.stderr
            import re
            
            qps_match = re.search(r'QPS: ([\d.]+)', output)
            latency_match = re.search(r'Latencies \(50/90/95/99/99\.9%-ile\): ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) us', output)
            
            if qps_match and latency_match:
                print(f"TCP Results: {qps_match.group(1)} QPS, {latency_match.group(1)}μs p50")
                return {
                    'qps': float(qps_match.group(1)),
                    'latency_50': float(latency_match.group(1)),
                    'latency_90': float(latency_match.group(2)),
                    'latency_95': float(latency_match.group(3)),
                    'latency_99': float(latency_match.group(4)),
                    'latency_999': float(latency_match.group(5))
                }
        else:
            print("TCP benchmark failed")
            print(client_proc.stderr)
            
    except Exception as e:
        print(f"TCP benchmark error: {e}")
        if 'server_proc' in locals():
            server_proc.terminate()
    
    return None

if __name__ == "__main__":
    result = run_tcp_benchmark()
    if result:
        print(f"TCP QPS: {result['qps']}")
        print(f"TCP P50 Latency: {result['latency_50']}μs")
