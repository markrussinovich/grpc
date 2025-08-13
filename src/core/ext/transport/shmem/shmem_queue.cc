#include "src/core/ext/transport/shmem/shmem_queue.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace grpc_shmem {

bool ReserveContiguous(DataRingBuffer* rb, uint32_t size, uint64_t* out_offset) {
	// size must be <= capacity and fit contiguously (we do not wrap inside a single reservation)
	if (size > rb->capacity) return false;
	for (;;) {
		const uint64_t head = rb->head.load(std::memory_order_acquire);
		const uint64_t tail = rb->tail.load(std::memory_order_acquire);
		const uint64_t used = head - tail;  // monotonic
			if (used + size > rb->capacity) {
				// Busy wait until sufficient free space becomes available.
				// Be polite and yield briefly to reduce starvation under load.
				std::this_thread::sleep_for(std::chrono::microseconds(50));
				continue;
			}
		// Reserve [head%capacity, head%capacity + size)
		const uint64_t offset = head % rb->capacity;
		// If this reservation would straddle the end, either wait or allow wrapping
		// by the next reservation. Here we choose to wait for simplicity.
			if (offset + size > rb->capacity) {
				std::this_thread::sleep_for(std::chrono::microseconds(50));
				continue;  // wait until consumer advances to free contiguous tail space
			}
		const uint64_t new_head = head + size;
		if (rb->head.compare_exchange_weak(
						const_cast<uint64_t&>(head), new_head, std::memory_order_acq_rel,
						std::memory_order_acquire)) {
			*out_offset = offset;
			return true;
		}
		// CAS raced, retry
	}
}

static inline void Post(ControlBlock* cb, Direction dir) {
	if (dir == Direction::kC2S) cb->c2s_sem.post();
	else cb->s2c_sem.post();
}

static inline void Wait(ControlBlock* cb, Direction dir) {
	if (dir == Direction::kC2S) cb->c2s_sem.wait();
	else cb->s2c_sem.wait();
}

bool PushCommand(ShmemQueues* q, ControlBlock* cb, Direction dir, const Command& cmd) {
	// Peek if queue is empty to decide on wakeup.
	// There is no direct empty() API that is lock-free, so approximate by trying a pop.
	// Instead, we rely on push() return value and signal unconditionally is too costly.
	// We'll signal if queue was likely empty by performing a lightweight try-pop via
	// cache. Since spsc_queue lacks size(), we do a best-effort: signal every push.
	// Optimization: try not to spam signals by signaling only when push succeeds.
	const bool ok = q->command_q.push(cmd);
	if (ok) {
		// Hybrid policy can afford spurious wakeups; to keep it simple, post on every push.
		Post(cb, dir);
	}
	return ok;
}

bool PopCommandHybrid(ShmemQueues* q, ControlBlock* cb, Direction dir, int spin_iters, Command* out) {
	Command tmp;
	for (int i = 0; i < spin_iters; ++i) {
		if (q->command_q.pop(tmp)) {
			*out = tmp;
			return true;
		}
	}
	// Sleep until woken up by producer
	Wait(cb, dir);
	// Upon wake, try again (one attempt)
	if (q->command_q.pop(tmp)) {
		*out = tmp;
		return true;
	}
	return false;
}

}  // namespace grpc_shmem
