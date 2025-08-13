// Queue/data-path helpers for shmem transport (producer/consumer logic).
#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H

#include <cstddef>
#include <cstdint>

#include "src/core/ext/transport/shmem/shmem_transport.h"

namespace grpc_shmem {

enum class Direction { kC2S, kS2C };

// Compute free space in the ring (in bytes), using monotonic head/tail.
inline uint64_t RingFreeBytes(const DataRingBuffer& rb) {
	const uint64_t head = rb.head.load(std::memory_order_acquire);
	const uint64_t tail = rb.tail.load(std::memory_order_acquire);
	const uint64_t used = head - tail;  // monotonic counters
	if (used > rb.capacity) return 0;   // saturated -> treat as full
	return rb.capacity - used;
}

// Reserve a contiguous region of 'size' bytes in the ring buffer.
// Returns true and fills out_offset (0..capacity-1) on success. Blocks by
// spinning until space is available. This function is SPSC-safe.
bool ReserveContiguous(DataRingBuffer* rb, uint32_t size, uint64_t* out_offset);

// Release 'size' bytes previously consumed starting from some offset; simply
// advance tail (consumer side responsibility).
inline void Release(DataRingBuffer* rb, uint32_t size) {
	rb->tail.fetch_add(size, std::memory_order_release);
}

// Push a command and optionally wake the sleeping peer if the queue was empty
// before the push. Returns true if the command was pushed.
bool PushCommand(ShmemQueues* q, ControlBlock* cb, Direction dir, const Command& cmd);

// Pop a command using hybrid spin-then-wait. Spins for spin_iters attempts;
// upon empty, waits on the appropriate semaphore. Returns true with a command
// when successful.
bool PopCommandHybrid(ShmemQueues* q, ControlBlock* cb, Direction dir, int spin_iters, Command* out);

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_QUEUE_H
