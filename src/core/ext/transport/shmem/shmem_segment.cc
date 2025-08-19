// Copyright 2025 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "src/core/ext/transport/shmem/shmem_segment.h"

#include <string.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <errno.h>
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "src/core/ext/transport/shmem/shmem_semaphore.h"

namespace grpc_shmem {

// -------- platform helpers --------

int ShmemSegment::CreateFd(const std::string& name, size_t size, std::string* created_name) {
  // Try memfd_create first if available, fallback to shm_open
  int fd = -1;
  
#ifdef __linux__
  // Try memfd_create if available (Linux 3.17+)
  #ifndef MFD_CLOEXEC
  #define MFD_CLOEXEC 0x0001U
  #endif
  #ifdef __NR_memfd_create
  fd = static_cast<int>(syscall(__NR_memfd_create, name.c_str(), MFD_CLOEXEC));
  if (fd != -1) {
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) { ::close(fd); return -1; }
    if (created_name) *created_name = name;
    return fd;
  }
  #endif
#endif

  // Fallback to shm_open on all POSIX systems
  std::string shm_name = "/" + name;
  fd = ::shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd == -1) return -1;
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) { ::close(fd); return -1; }
  if (created_name) *created_name = shm_name;
  return fd;
}

int ShmemSegment::OpenFd(const std::string& name, size_t* size_out) {
#ifdef __linux__
  (void)size_out; // not used for memfd
  // For memfd, we rely on caller to know size. If needed, fstat to get st_size.
  // This path is not currently used; keeping for completeness.
  return -1;
#else
  int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
  if (fd == -1) return -1;
  // Obtain size via fstat:
  struct stat st{};
  if (::fstat(fd, &st) != 0) { ::close(fd); return -1; }
  if (size_out) *size_out = static_cast<size_t>(st.st_size);
  return fd;
#endif
}

void* ShmemSegment::Map(int fd, size_t size) {
  void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (p == MAP_FAILED) return nullptr;
  return p;
}

void ShmemSegment::Unmap() {
  if (base_ != nullptr) {
    int munmap_result = ::munmap(base_, size_);
    if (munmap_result != 0) {
      int saved_errno = errno;
      LOG(ERROR) << "ShmemSegment::Unmap() munmap failed: base=" << base_ 
                 << ", size=" << size_ << ", errno=" << saved_errno 
                 << " (" << strerror(saved_errno) << ")";
      // Continue cleanup despite munmap failure
    }
    base_ = nullptr; size_ = 0;
  }
  if (fd_ != -1) { 
    int close_result = ::close(fd_); 
    if (close_result != 0) {
      int saved_errno = errno;
      LOG(ERROR) << "ShmemSegment::Unmap() close failed: fd=" << fd_ 
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      // Continue cleanup despite close failure
    }
    fd_ = -1; 
  }
}

// -------- public API --------

void ShmemSegment::RemoveIfExists(const std::string& name) {
#ifndef __linux__
  // Only shm_unlink() path needs explicit removal.
  std::string shm_name = "/" + name;
  ::shm_unlink(shm_name.c_str()); // ignore errors
#else
  (void)name; // memfd has no global name to remove
#endif
}

// Layout: [ControlBlock | ... rest for queues ...]
static inline size_t Align(size_t x, size_t a) { return (x + (a-1)) & ~(a-1); }

void ShmemSegment::InitQueues(void* base, size_t size, ControlBlock* cb,
                              std::size_t data_ring_capacity) {
  (void)size;
  // Initialize semaphores
  (void)cb->c2s_sem.Init(0, /*semaphore_mode=*/true);
  (void)cb->s2c_sem.Init(0, /*semaphore_mode=*/true);

  // Layout: [ControlBlock | ShmemQueues c2s | ShmemQueues s2c | c2s_data | s2c_data]
  char* mem = static_cast<char*>(base);
  size_t offset = Align(sizeof(ControlBlock), alignof(ShmemQueues));
  
  // Place ShmemQueues structures
  auto* c2s = reinterpret_cast<ShmemQueues*>(mem + offset);
  offset = Align(offset + sizeof(ShmemQueues), alignof(ShmemQueues));
  auto* s2c = reinterpret_cast<ShmemQueues*>(mem + offset);
  offset = Align(offset + sizeof(ShmemQueues), alignof(unsigned char));
  
  // Place data ring buffers
  auto* c2s_buf = reinterpret_cast<unsigned char*>(mem + offset);
  offset = Align(offset + data_ring_capacity, alignof(unsigned char));
  auto* s2c_buf = reinterpret_cast<unsigned char*>(mem + offset);

  // Initialize ShmemQueues structures
  new(c2s) ShmemQueues();
  new(s2c) ShmemQueues();

  // Initialize data ring buffers
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
  std::string created_name;
  int fd = CreateFd(cfg.name, cfg.size, &created_name);
  if (fd == -1) return {};
  void* base = Map(fd, cfg.size);
  if (base == nullptr) { ::close(fd); return {}; }

  // Place control block at the start.
  auto* cb = reinterpret_cast<ControlBlock*>(base);
  ::memset(cb, 0, sizeof(ControlBlock));
  
  // Initialize ControlBlock fields
  cb->magic_number = 0x47525043534D454Dull;  // "GRPCSMEM"
  cb->transport_version = 1;
  cb->server_state.store(1);  // listening
  cb->client_state.store(0);
  
  InitQueues(base, cfg.size, cb, cfg.data_ring_capacity);

  return ShmemSegment(created_name, base, cfg.size, cb, fd);
}

ShmemSegment ShmemSegment::Open(const std::string& /*name*/) {
  // Cross-process Open() isn't used in the current in-proc design; return empty.
  return {};
}

}  // namespace grpc_shmem