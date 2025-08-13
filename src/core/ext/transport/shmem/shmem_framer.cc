#include "src/core/ext/transport/shmem/shmem_framer.h"

#include <cstring>

namespace grpc_shmem {

namespace {
struct TailRelease {
	DataRingBuffer* rb;
	uint32_t size;
};

void ReleaseTail(void* ud) {
	auto* tr = static_cast<TailRelease*>(ud);
	if (tr != nullptr && tr->rb != nullptr && tr->size > 0) {
		tr->rb->tail.fetch_add(tr->size, std::memory_order_release);
	}
	delete tr;
}
}  // namespace

grpc_slice MakeSliceFromRing(DataRingBuffer* rb, uint64_t offset, uint32_t size) {
	// Note: we assume caller guaranteed contiguous region [offset, offset+size)
	// within the ring bounds (offset+size <= capacity).
	unsigned char* ptr = rb->buffer.get() + offset;
	auto* ud = new TailRelease{rb, size};
	return grpc_slice_new_with_user_data(ptr, size, &ReleaseTail, ud);
}

}  // namespace grpc_shmem
