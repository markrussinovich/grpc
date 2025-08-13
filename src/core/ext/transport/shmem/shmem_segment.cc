#include "src/core/ext/transport/shmem/shmem_segment.h"

#include <cstring>

namespace bip = boost::interprocess;

namespace grpc_shmem {

namespace {
constexpr const char* kControlBlockName = "grpc_shmem_control";
constexpr const char* kC2SQueuesName = "grpc_shmem_c2s_queues";
constexpr const char* kS2CQueuesName = "grpc_shmem_s2c_queues";
constexpr const char* kC2SDataName = "grpc_shmem_c2s_data";
constexpr const char* kS2CDataName = "grpc_shmem_s2c_data";
}  // namespace

void ShmemSegment::InitQueues(bip::managed_shared_memory& seg, ControlBlock* cb,
															std::size_t data_ring_capacity) {
	// Construct ShmemQueues for each direction
	auto* c2s = seg.find_or_construct<ShmemQueues>(kC2SQueuesName)();
	auto* s2c = seg.find_or_construct<ShmemQueues>(kS2CQueuesName)();

	// Allocate data ring buffers
	auto* c2s_buf = seg.construct<unsigned char>(kC2SDataName)[data_ring_capacity]();
	auto* s2c_buf = seg.construct<unsigned char>(kS2CDataName)[data_ring_capacity]();

	c2s->data_rb.capacity = data_ring_capacity;
	c2s->data_rb.head.store(0);
	c2s->data_rb.tail.store(0);
	c2s->data_rb.buffer = c2s_buf;

	s2c->data_rb.capacity = data_ring_capacity;
	s2c->data_rb.head.store(0);
	s2c->data_rb.tail.store(0);
	s2c->data_rb.buffer = s2c_buf;

	// Store pointers in control block
	cb->c2s_queues = c2s;
	cb->s2c_queues = s2c;
}

ShmemSegment ShmemSegment::Create(const SegmentConfig& cfg) {
	// Ensure any previous segment is removed for a clean start in tests/examples.
	RemoveIfExists(cfg.name);

	// Create or open managed shared memory
	auto seg = std::make_unique<bip::managed_shared_memory>(bip::create_only, cfg.name.c_str(), cfg.size);

	// Construct ControlBlock
	auto* cb = seg->find_or_construct<ControlBlock>(kControlBlockName)();
	cb->magic_number = 0x47525043534D454Dull;
	cb->transport_version = 1;
	cb->server_state.store(1);  // listening
	cb->client_state.store(0);
	// Semaphores already initialized to 0 in constructor.

	// Initialize queues and data rings
	InitQueues(*seg, cb, cfg.data_ring_capacity);

	return ShmemSegment(cfg.name, std::move(seg), cb);
}

ShmemSegment ShmemSegment::Open(const std::string& name) {
	auto seg = std::make_unique<bip::managed_shared_memory>(bip::open_only, name.c_str());

	auto res = seg->find<ControlBlock>(kControlBlockName);
	ControlBlock* cb = nullptr;
	if (res.first != nullptr) cb = res.first;

	// Basic verification
	if (cb != nullptr && cb->magic_number == 0x47525043534D454Dull && cb->transport_version == 1) {
		cb->client_state.store(1);
	}

	return ShmemSegment(name, std::move(seg), cb);
}

}  // namespace grpc_shmem
