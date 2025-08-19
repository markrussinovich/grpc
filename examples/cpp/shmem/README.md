gRPC Shared Memory Example
================

This example shows how to use gRPC with shared memory transport.
gRPC uses the [`shmem:segment_path`](https://github.com/grpc/grpc/blob/c6844099218b147b0e374843e0a26745adc61ddb/doc/naming.md?plain=1#L44-L50) URI scheme to support this.
In this example, a shared memory segment `grpc_shmem_example` is created.

## Build and run the example

Run `bazel run :server` in one terminal, and `bazel run :client` in another.

The client and server will confirm that a message was sent and received on both ends. The server will continue running until it is shut down.
While the server is still running, you can confirm that a shared memory segment is in use by running `ipcs -m | grep grpc_shmem_example`.