# gRPC Shmem Transport Development Notes

## Important Architecture Principles

### SHMEM IS CROSS-PROCESS PROTOCOL - FOLLOW TCP DESIGN
- **CRITICAL**: Shmem transport is a cross-process protocol using shared memory segments and semaphores
- **DO NOT look at inproc** - inproc is in-process only, shmem is cross-process
- **Copy TCP transport design** - TCP uses network sockets, shmem uses shared memory, but both are cross-process
- Use TCP transport as the reference architecture for async integration

## Message Chunking Implementation

The shmem transport now supports messages larger than ring buffer capacity through chunking:

### Frame Types Added
- `C2S_MESSAGE_CHUNK` (0x31), `C2S_MESSAGE_CHUNK_LAST` (0x32) - Client to server chunks
- `S2C_MESSAGE_CHUNK` (0x41), `S2C_MESSAGE_CHUNK_LAST` (0x42) - Server to client chunks

### Implementation
- **Sender-side**: Messages >ring capacity split into ~64KB chunks with proper PAD handling
- **Receiver-side**: SliceBuffer accumulators reassemble chunks into complete messages
- **Zero-copy**: Uses `MakeSliceFromRing()` to avoid data copying during reassembly
- **Automatic cleanup**: Chunk accumulators cleaned up after complete message assembly

## Files Modified
- `/datadrive/grpc/src/core/ext/transport/shmem/shmem_transport.cc` - Main shmem transport implementation

## Testing
- Unary benchmark: `./bazel-bin/test/cpp/microbenchmarks/bm_fullstack_unary_ping_pong --benchmark_filter="BM_UnaryPingPong<ShmemTransport.*>/0/0"`
- 16MB messages: 399 MB/s (working)
- 128MB messages: Chunking implementation complete, ready for async integration testing