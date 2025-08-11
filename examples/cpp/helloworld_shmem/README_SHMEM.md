# Shared Memory Transport Demo

This directory contains a demonstration of the shared-memory (shmem) transport implementation for gRPC Core.

## What's in this demo

- `shmem_demo.cc` - A simple demonstration that creates a shared-memory transport pair and validates that the transport is working correctly.
- `greeter_server.cc` and `greeter_client.cc` - Modified versions of the standard helloworld example that attempt to use the shmem transport (currently not functional due to scheme registration limitations).

## Building and Running

To build the demo:
```bash
bazel build //examples/cpp/helloworld_shmem:shmem_demo
```

To run the demo:
```bash
./bazel-bin/examples/cpp/helloworld_shmem/shmem_demo
```

Expected output:
```
[shmem demo] Starting shared-memory transport demo...
[shmem demo] Transport pair created successfully!
[shmem demo] Client transport: 0x...
[shmem demo] Server transport: 0x...
[shmem demo] Demo complete - shared-memory transport is working!
```

## Technical Notes

The shmem transport is implemented as a low-level transport that works by creating paired client/server transports that communicate through shared memory segments. The transport includes:

- **Shared memory segments** for data exchange
- **SPSC queues** for efficient lock-free communication
- **Custom frame protocol** with headers, metadata, and message fragmentation
- **Metadata and trailer support** with encoding/decoding
- **Multiplexed stream handling** for concurrent operations
- **Cancellation and flow control** support

## Tests

The shmem transport has comprehensive tests that validate:
- Basic unary operations
- Metadata and trailer handling  
- Fragmentation and reassembly
- Concurrency and thread safety
- Error conditions and edge cases

Run the tests with:
```bash
bazel test //test/core/transport:shmem_e2e_test
bazel test //test/core/transport:shmem_e2e_concurrency_test
bazel test //test/core/transport:shmem_framer_test
bazel test //test/core/transport:shmem_queue_test
bazel test //test/core/transport:shmem_segment_test
```

## Limitations

Currently, the shmem transport can only be used directly via the `MakeShmemTransportPair()` factory function and is not registered as a URI scheme (like `shmem://`). This means it cannot be used with the high-level gRPC C++ APIs like `grpc::CreateChannel("shmem://...")`. Future work could add scheme registration to enable seamless integration with existing gRPC applications.
