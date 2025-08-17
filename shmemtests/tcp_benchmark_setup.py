#!/usr/bin/env python3
"""
Proper TCP benchmark setup using QPS workers.
"""

import subprocess
import time
import signal
import os
import json
from typing import List, Optional

class TCPBenchmarkSetup:
    def __init__(self):
        self.worker_processes = []
        self.server_port = 10000
        self.client_ports = [10001, 10002]
    
    def start_qps_workers(self) -> bool:
        """Start QPS worker processes for distributed testing."""
        try:
            # Start server worker
            print(f"Starting server worker on port {self.server_port}...")
            server_proc = subprocess.Popen([
                "./bazel-bin/test/cpp/qps/qps_worker",
                f"--driver_port={self.server_port}"
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.worker_processes.append(server_proc)
            
            # Start client workers
            for port in self.client_ports:
                print(f"Starting client worker on port {port}...")
                client_proc = subprocess.Popen([
                    "./bazel-bin/test/cpp/qps/qps_worker", 
                    f"--driver_port={port}"
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.worker_processes.append(client_proc)
            
            # Give workers time to start
            time.sleep(3)
            
            # Set environment variable
            workers = [f"localhost:{self.server_port}"] + [f"localhost:{p}" for p in self.client_ports]
            os.environ["QPS_WORKERS"] = ",".join(workers)
            print(f"Set QPS_WORKERS={os.environ['QPS_WORKERS']}")
            
            return True
            
        except Exception as e:
            print(f"Failed to start QPS workers: {e}")
            self.cleanup()
            return False
    
    def cleanup(self):
        """Stop all worker processes."""
        for proc in self.worker_processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
        self.worker_processes.clear()
    
    def run_tcp_benchmark(self) -> Optional[dict]:
        """Run a TCP benchmark scenario."""
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
                "warmup_seconds": 3,
                "benchmark_seconds": 5
            }]
        }
        
        try:
            print("Running TCP benchmark...")
            result = subprocess.run([
                "./bazel-bin/test/cpp/qps/qps_json_driver",
                f"--scenarios_json={json.dumps(scenario)}"
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print("TCP benchmark completed successfully!")
                print("Output:", result.stderr[-500:])  # Show last 500 chars
                return {"success": True, "output": result.stderr}
            else:
                print("TCP benchmark failed:")
                print("Error:", result.stderr[-500:])
                return {"success": False, "error": result.stderr}
                
        except subprocess.TimeoutExpired:
            print("TCP benchmark timed out")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            print(f"TCP benchmark exception: {e}")
            return {"success": False, "error": str(e)}

def main():
    print("=== TCP Benchmark Setup Demo ===")
    print("This shows how to properly set up TCP benchmarks with QPS workers.\n")
    
    # Check if QPS binaries exist
    if not os.path.exists("./bazel-bin/test/cpp/qps/qps_worker"):
        print("Error: QPS worker binary not found")
        print("Build with: bazel build test/cpp/qps:qps_worker")
        return
    
    if not os.path.exists("./bazel-bin/test/cpp/qps/qps_json_driver"):
        print("Error: QPS driver binary not found")  
        print("Build with: bazel build test/cpp/qps:qps_json_driver")
        return
    
    setup = TCPBenchmarkSetup()
    
    try:
        # Start workers
        if setup.start_qps_workers():
            # Run benchmark
            result = setup.run_tcp_benchmark()
            if result and result["success"]:
                print("\n✅ TCP benchmark completed successfully!")
            else:
                print("\n❌ TCP benchmark failed")
        else:
            print("❌ Failed to start QPS workers")
    
    finally:
        # Cleanup
        print("\nCleaning up workers...")
        setup.cleanup()
        print("Done!")

if __name__ == "__main__":
    main()
