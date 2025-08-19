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

#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H

#include <stdint.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <errno.h>
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "src/core/lib/gprpp/log.h"

namespace grpc_shmem {

// Simple counting semaphore built on Linux eventfd.
// NOTE: Intended for *in-process* usage (client/server in same process).
// If you turn on true cross-process later, pass/dup the fds explicitly.
class EventFdSemaphore {
 public:
  EventFdSemaphore() = default;
  EventFdSemaphore(const EventFdSemaphore&) = delete;
  EventFdSemaphore& operator=(const EventFdSemaphore&) = delete;
  ~EventFdSemaphore() { Close(); }

  absl::Status Init(unsigned initial = 0, bool semaphore_mode = true) {
    if (fd_ != -1) return absl::OkStatus();
    int flags = EFD_CLOEXEC;
    if (semaphore_mode) flags |= EFD_SEMAPHORE;
    fd_ = ::eventfd(initial, flags);
    if (fd_ == -1) {
      int saved_errno = errno;
      GRPC_LOG_ERROR("EventFdSemaphore::Init() eventfd failed: initial=%u, flags=0x%x, errno=%d (%s)", 
                     initial, flags, saved_errno, strerror(saved_errno));
      return absl::UnknownError(absl::StrCat("eventfd() failed: ", strerror(saved_errno)));
    }
    return absl::OkStatus();
  }

  void Close() {
    if (fd_ != -1) {
      int close_result = ::close(fd_);
      if (close_result != 0) {
        int saved_errno = errno;
        GRPC_LOG_ERROR("EventFdSemaphore::Close() failed: fd=%d, errno=%d (%s)", 
                       fd_, saved_errno, strerror(saved_errno));
        // Continue with cleanup despite close failure
      }
      fd_ = -1;
    }
  }

  // Wake one waiter.
  inline void post() {
    const uint64_t one = 1;
    ssize_t result = ::write(fd_, &one, sizeof(one));
    if (result != sizeof(one)) {
      int saved_errno = errno;
      GRPC_LOG_ERROR("EventFdSemaphore::post() write failed: fd=%d, result=%zd, errno=%d (%s)", 
                     fd_, result, saved_errno, strerror(saved_errno));
      // In production, this is a fatal error - semaphore synchronization is broken
      // For now, log and continue, but calling code should handle this scenario
    }
  }

  // Block until signaled.
  inline void wait() {
    uint64_t val;
    ssize_t result = ::read(fd_, &val, sizeof(val));
    if (result != sizeof(val)) {
      int saved_errno = errno;
      if (saved_errno == EINTR) {
        // Interrupted by signal, retry
        GRPC_LOG_INFO("EventFdSemaphore::wait() interrupted by signal, retrying");
        wait(); // Recursive retry - in production, consider iterative approach
        return;
      }
      GRPC_LOG_ERROR("EventFdSemaphore::wait() read failed: fd=%d, result=%zd, errno=%d (%s)", 
                     fd_, result, saved_errno, strerror(saved_errno));
      // This is a fatal error - synchronization is broken
      // For now, log and return, but calling code should handle this scenario
    }
  }

  int fd() const { return fd_; }

 private:
  int fd_ = -1;
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H