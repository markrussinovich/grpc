# Shared Memory Transport

This directory contains a shared memory transport implementation for gRPC.

See also: [gRPC Transports overview](../../transport/GEMINI.md)

## Overarching Purpose

The shared memory transport provides a mechanism for a client and server to communicate between different processes on the same machine using shared memory. This transport is optimized for high-performance inter-process communication (IPC) scenarios where traditional TCP/UDS overhead is undesirable. It leverages shared memory segments, lock-free queues, and lightweight synchronization primitives to achieve low-latency, high-throughput communication.

## Architecture Overview

The shared memory transport uses a dual-queue design with a shared data ring buffer:

- **Command Queues**: Bidirectional lock-free queues (client-to-server and server-to-client) for control messages
- **Data Ring Buffer**: Shared circular buffer for efficient large message transfer
- **Control Block**: Shared metadata structure containing synchronization primitives and transport state
- **Lightweight Synchronization**: Uses Linux eventfd-based semaphores for thread wake-up coordination

## Files

### Core Transport Implementation
* `shmem_transport.h`, `shmem_transport.cc`: Main transport implementation containing `ShmemClientTransport` and `ShmemServerTransport` classes
* `shmem_channel.cc`: Channel creation and transport pair setup logic
* `shmem_transport_stub.cc`: Transport registration and factory functions

### Memory Management
* `shmem_memory.h`, `shmem_memory.cc`: Shared memory segment allocation and management utilities
* `shmem_segment.h`, `shmem_segment.cc`: Low-level shared memory segment wrapper and lifecycle management

### Protocol Components
* `shmem_protocol.h`: Protocol constants, magic numbers, and configuration defaults
* `shmem_framer.h`, `shmem_framer.cc`: Message framing and serialization for the shared memory protocol
* `shmem_queue.h`, `shmem_queue.cc`: Lock-free queue implementation using custom SPSC queue
* `shmem_lockfree_queue.h`: Custom single-producer single-consumer lock-free queue implementation

### Build Configuration
* `BUILD`: Bazel build configuration defining the shmem transport library and dependencies

## Major Functions

* `grpc_core::MakeShmemTransportPair`: Creates a pair of connected shared memory transports. This is the main entry point for creating shmem transport connections.
* `grpc_shmem_channel_create`: Creates a shared memory channel. This is a convenience function that wraps the transport pair creation.
* `ShmemClientTransport::StartCall`: Initiates a new RPC call over the shared memory transport.
* `ShmemServerTransport::AcceptStream`: Accepts incoming RPC streams from clients.

## Key Features

### Performance Optimizations
* **Lock-free queues**: Uses custom SPSC (single-producer single-consumer) queue for command passing
* **Zero-copy data transfer**: Large messages use shared ring buffer to avoid copying
* **Configurable spinning**: Optional busy-waiting to reduce latency for high-frequency workloads
* **Dispatch-only mode**: Optimized for event-driven applications

### Synchronization Model
* **Lightweight semaphores**: EventFdSemaphore using Linux eventfd() for thread coordination
* **Atomic state management**: Lock-free connection state tracking
* **Efficient wake-up**: Producers only signal when consumers are waiting

### Memory Layout
```
Shared Memory Segment:
┌─────────────────┐
│ Control Block   │ ← Magic number, version, semaphores, state
├─────────────────┤
│ C2S Queue       │ ← Client-to-server command queue
├─────────────────┤
│ S2C Queue       │ ← Server-to-client command queue  
├─────────────────┤
│ Data Ring       │ ← Circular buffer for large messages
└─────────────────┘
```

## Configuration Options

The shmem transport supports the following channel arguments for performance tuning:

### `grpc.shmem.spin_iters` (integer)
Controls the number of busy-wait iterations before falling back to blocking synchronization.

* **Type**: `int`
* **Default**: `0` (no spinning, optimized for event-driven workloads)
* **Valid Range**: `0` to `10000`
* **Validation**: Values outside the valid range are clamped with warnings logged
* **Purpose**: Higher values reduce latency for high-frequency messaging at the cost of CPU usage

**Usage Guidelines:**
- `0`: Best for event-driven applications, servers with many idle connections
- `1-50`: Good balance for moderate load applications  
- `100-1000`: Low-latency applications willing to trade CPU for reduced latency
- `1000-10000`: High-frequency trading or other latency-critical applications

**Example:**
```cpp
grpc::ChannelArguments args;
args.SetInt("grpc.shmem.spin_iters", 100);  // Low-latency mode
```

### `grpc.shmem.dispatch_only` (boolean)  
Controls whether the transport should only operate in dispatch-only mode.

* **Type**: `bool`
* **Default**: `true`
* **Purpose**: When enabled, ensures compatibility with gRPC's event-driven architecture
* **Note**: This is primarily an internal configuration option. Most users should leave this at the default value.

**Example:**
```cpp
grpc::ChannelArguments args;
args.SetInt("grpc.shmem.dispatch_only", 1);  // Enable (default)
```

### Error Handling and Validation

All channel arguments are validated during transport initialization:
- Invalid values generate warning logs with details about the validation failure
- Out-of-range values are automatically clamped to safe bounds  
- Default values are used when invalid arguments are provided
- Validation errors do not prevent transport initialization

## Protocol Specification

* **Magic Number**: `0x47525043534D454D` ("GRPCSMEM")
* **Version**: 1
* **Default Data Ring Size**: 4 MiB
* **Maximum Message Size**: 3 MiB

## Notes

* The shared memory transport is designed for high-performance inter-process communication on the same machine. It is not suitable for network communication.
* The transport uses POSIX shared memory (memfd_create/shm_open + mmap) for efficient memory management on Linux and other POSIX systems.
* The lock-free queue design ensures high throughput and low latency, making it suitable for latency-critical applications.
* Memory safety is ensured through careful lifetime management of shared memory segments and proper cleanup on transport destruction.
* The transport supports both streaming and unary RPC patterns with full gRPC feature compatibility.
* Performance testing shows significant improvements over TCP and UDS transports for large messages and high-frequency communication patterns.