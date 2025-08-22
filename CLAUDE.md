# gRPC Shmem Transport Development Notes

## Important Architecture Principles

### SHMEM IS CROSS-PROCESS PROTOCOL - FOLLOW TCP DESIGN
- **CRITICAL**: Shmem transport is a cross-process protocol using shared memory segments and semaphores
- **DO NOT look at inproc** - inproc is in-process only, shmem is cross-process
- **Copy TCP transport design** - TCP uses network sockets, shmem uses shared memory, but both are cross-process
- Use TCP transport as the reference architecture for async integration

## Current Issue: Async Integration Missing Response Bridge

The shmem transport successfully:
1. Calls `accept_stream_cb` to notify gRPC server of new requests ✅
2. Uses `d->StartCall()` to deliver requests to async server ✅
3. Server processes requests internally ✅

**Missing piece**: Server responses need to flow back through S2C shmem protocol to client transport, just like TCP server responses flow back through network socket to client.

### Root Cause
- `d->StartCall()` processes request server-side but doesn't send response back through shmem S2C queues
- Client transport waits in completion queue but never receives completion events
- Need to implement server response bridge to send async server responses through S2C protocol

### Solution Direction
Implement callback mechanism that:
1. Captures when async server call completes
2. Extracts server response (metadata, message, status)  
3. Sends response through S2C shmem queues
4. Client transport receives S2C frames and triggers completion callbacks

## Files Modified
- `/datadrive/grpc/src/core/ext/transport/shmem/shmem_transport.cc` - Main shmem transport implementation

## Testing
- Unary benchmark: `./bazel-bin/test/cpp/microbenchmarks/bm_fullstack_unary_ping_pong --benchmark_filter="BM_UnaryPingPong<ShmemTransport.*>/0/0"`
- InProcess benchmark works (~370ms): proves benchmark infrastructure is correct
- Shmem benchmark hangs in `grpc_completion_queue_next`: missing response bridge