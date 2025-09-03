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
  // For cross-process access, always use shm_open to create a named segment
  // that both server and client processes can access
  std::string shm_name = (!name.empty() && name[0] == '/') ? name : "/" + name;
  
  // Remove existing segment if it exists
  ::shm_unlink(shm_name.c_str());
  
  int fd = ::shm_open(shm_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (fd == -1) return -1;
  
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) { 
    ::close(fd); 
    ::shm_unlink(shm_name.c_str());
    return -1; 
  }
  
  if (created_name) *created_name = shm_name;
  return fd;
}

int ShmemSegment::OpenFd(const std::string& name, size_t* size_out) {
  // For cross-process access, we need to use shm_open on all platforms
  std::string shm_name = (!name.empty() && name[0] == '/') ? name : "/" + name;
  int fd = ::shm_open(shm_name.c_str(), O_RDWR, 0600);
  if (fd == -1) return -1;
  
  // Obtain size via fstat
  struct stat st{};
  if (::fstat(fd, &st) != 0) { 
    ::close(fd); 
    return -1; 
  }
  if (size_out) *size_out = static_cast<size_t>(st.st_size);
  return fd;
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
  
  // Unlink the shared memory file to prevent resource leaks (only if enabled)
  if (!name_.empty() && should_unlink_on_destroy_) {
    VLOG(1) << "ShmemSegment::Unmap() unlinking segment: " << name_ << " (PID=" << getpid() << ")";
    int unlink_result = ::shm_unlink(name_.c_str());
    if (unlink_result != 0) {
      int saved_errno = errno;
      // Don't log ENOENT as error - segment may have been already unlinked
      if (saved_errno != ENOENT) {
        LOG(WARNING) << "ShmemSegment::Unmap() shm_unlink failed: name=" << name_
                     << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      } else {
        VLOG(2) << "ShmemSegment::Unmap() segment already unlinked: " << name_;
      }
    } else {
      VLOG(1) << "ShmemSegment::Unmap() successfully unlinked: " << name_;
    }
  } else if (!name_.empty()) {
    VLOG(2) << "ShmemSegment::Unmap() skipping unlink for: " << name_ << " (should_unlink=" << (should_unlink_on_destroy_ ? "true" : "false") << ")";
  }
}

// -------- public API --------

void ShmemSegment::RemoveIfExists(const std::string& name) {
  // Always use shm_unlink since we use shm_open for cross-process segments
  // (The old code incorrectly assumed Linux used memfd, but we use shm_open everywhere)
  std::string shm_name = (!name.empty() && name[0] == '/') ? name : "/" + name;
  int result = ::shm_unlink(shm_name.c_str());
  if (result != 0 && errno != ENOENT) {
    // Log warning but continue - ENOENT is expected if already removed
    VLOG(2) << "RemoveIfExists: shm_unlink failed for " << shm_name 
            << ", errno=" << errno << " (" << strerror(errno) << ")";
  } else if (result == 0) {
    VLOG(1) << "RemoveIfExists: successfully removed " << shm_name;
  }
}

void ShmemSegment::RemoveNamedSemaphores(const std::string& server_name) {
  CrossProcessSemaphore::UnlinkNamed(server_name + "_c2s");
  CrossProcessSemaphore::UnlinkNamed(server_name + "_s2c");
}

// Layout: [ControlBlock | ... rest for queues ...]
static inline size_t Align(size_t x, size_t a) { return (x + (a-1)) & ~(a-1); }

void ShmemSegment::InitQueues(void* base, size_t size, ControlBlock* cb,
                              std::size_t data_ring_capacity, 
                              const std::string& server_name) {
  (void)size;
  // Initialize semaphore names for cross-process usage
  if (!server_name.empty()) {
    // Store semaphore names in shared memory instead of process-specific handles
    std::string c2s_name = "/" + server_name + "_c2s";
    std::string s2c_name = "/" + server_name + "_s2c";
    
    // Futex doorbells are initialized directly in ControlBlock struct - no setup needed
    LOG(INFO) << "Using futex doorbells for server: " << server_name;
  } else {
    // In-process mode also uses futex doorbells
    LOG(INFO) << "Using futex doorbells for in-process mode";
  }

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

  // Store offsets in control block (relative to segment base for cross-process compatibility)
  char* segment_base = reinterpret_cast<char*>(cb);
  
  // Initialize data ring buffers with cross-process compatible offsets
  c2s->data_rb.capacity = data_ring_capacity;
  c2s->data_rb.head.store(0);
  c2s->data_rb.tail.store(0);
  c2s->data_rb.buffer_offset = reinterpret_cast<char*>(c2s_buf) - segment_base;

  s2c->data_rb.capacity = data_ring_capacity;
  s2c->data_rb.head.store(0);
  s2c->data_rb.tail.store(0);
  s2c->data_rb.buffer_offset = reinterpret_cast<char*>(s2c_buf) - segment_base;
  cb->c2s_queues_offset = reinterpret_cast<char*>(c2s) - segment_base;
  cb->s2c_queues_offset = reinterpret_cast<char*>(s2c) - segment_base;
}

ShmemSegment ShmemSegment::Create(const SegmentConfig& cfg) {
  std::string created_name;
  int fd = CreateFd(cfg.name, cfg.size, &created_name);
  if (fd == -1) return {};
  
  void* base = Map(fd, cfg.size);
  if (base == nullptr) { 
    ::close(fd); 
    return {}; 
  }
  
  // Place control block at the start and initialize it properly
  auto* cb = new(base) ControlBlock{};  // Value-initialize (zeros atomics & POD)
  
  // Set magic/version during creation using atomic stores
  cb->magic_number.store(0x47525043534D454Dull, std::memory_order_release); // "GRPCSMEM"
  cb->transport_version.store(1, std::memory_order_release);
  
  // Explicitly zero-initialize doorbell atomics (belt-and-suspenders)
  cb->c2s_db.seq.store(0, std::memory_order_relaxed);
  cb->c2s_db.waiter.store(0, std::memory_order_relaxed);
  cb->s2c_db.seq.store(0, std::memory_order_relaxed);
  cb->s2c_db.waiter.store(0, std::memory_order_relaxed);
  
  // Set additional fields
  cb->server_state.store(1);  // listening
  cb->client_state.store(0);
  
  InitQueues(base, cfg.size, cb, cfg.data_ring_capacity, cfg.server_name);

  return ShmemSegment(created_name, base, cfg.size, cb, fd);
}

ShmemSegment ShmemSegment::Open(const std::string& name) {
  size_t size = 0;
  int fd = OpenFd(name, &size);
  if (fd == -1) return {};
  
  void* base = Map(fd, size);
  if (base == nullptr) { 
    ::close(fd); 
    return {};
  }

  // Verify the control block using atomic load
  auto* cb = reinterpret_cast<ControlBlock*>(base);
  if (cb->magic_number.load(std::memory_order_acquire) != 0x47525043534D454Dull) {  // "GRPCSMEM"
    ::munmap(base, size);
    ::close(fd);
    return {};
  }

  // Verify that semaphore names are properly set in shared memory
  // (They should be initialized by the server during segment creation)

  // Mark client as connected
  cb->client_state.store(1);
  
  return ShmemSegment(name, base, size, cb, fd);
}

}  // namespace grpc_shmem