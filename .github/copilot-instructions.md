# Copilot Agent Project Instructions

## Project Overview
This repository contains the gRPC library. The work is focused on implementing a **shared memory (shmem) transport** with the following goals:  

1. **Minimize data copies** — ideally to just one copy.  
2. **Minimize kernel calls**.  
3. **Performance target** — should approach that of the `inproc` transport while supporting inter-process communication.  

The shared memory transport will be used by the gRPC **server and client** when they are on the **same machine**. If the client and server are not on the same machine, gRPC should fall back to another transport such as TCP.  

Requirements:  
- Must use shared memory for exchanging data.  
- Must implement and default to the **v3 protocol**, while supporting legacy protocols for benchmark purposes.  

---

## Code Style and Standards
- Follow gRPC coding conventions as outlined in the **`CONTRIBUTING.md`** file.  
- Ensure changes are well-documented with **inline comments** where design or performance trade-offs are made.  
- All public APIs or major design choices should be explained in accompanying documentation.  

---

## Project Structure and Key Files
- **`shmemtests/`**: Contains tests for the shared memory transport.  
- **`src/core/ext/transport/shmem/`**: Contains the implementation of the shared memory transport.  

---

## Validation and CI
- All changes must pass the **sync and async client/server examples** in `shmemtests/`.  
- The **umary ping-ping test** with shmem must run cleanly with full optimazions and no debug prints with **no errors and no hangs**.  
- Add new test cases for any edge conditions introduced.  
- Benchmark results should be collected and compared against both `inproc` and TCP transports to confirm performance goals.  

---

## Special Notes
- **No polling** — all notifications must be via **futexes**.  
- **v3 support** — the transport must correctly implement v3 promise support, but also support legacy for running the grpc benchmarks. It should detect which implementation to use based on the client/server configuration.
- **No sleeps** — sleeps just mask race conditions, they don't fix them.
- **No dedicated reader threads** — transport should be fully **event-driven** and integrated into the gRPC event notification system.  
- **Do not deviate from user requests** without explicit approval.  
- **Use logging instead of prints for debugging messages**
- **Wait for builds to complete even if they take long time**
- Create tempoary test files in a `/tmp` directory to avoid cluttering the repository.
- Always provide a **list of planned steps** before starting implementation.  
  - Make **incremental changes** and validate them at each step.  
  - Ensure at each step you have not deviated from the project goals.  
- All concurrency mechanisms must be well-tested for **deadlock/livelock safety** and **fairness**.  

---

## Additional Guidance
- **Error handling**: Provide clear recovery paths for shared memory setup failures (e.g., allocation, permission errors).  
- **Resource cleanup**: Ensure shared memory segments and futexes are properly destroyed after use to prevent leaks.  
- **Cross-version compatibility**: Document how legacy protocol support is implemented and tested.  
- **Performance validation**: Collect metrics (latency, throughput, CPU usage) to validate optimizations.  
- **Security**: Consider shared memory access restrictions (e.g., process permissions) to prevent data leakage across unrelated processes.  
