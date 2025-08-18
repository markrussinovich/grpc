#ifndef GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H
#define GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H

#include <stdint.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "absl/status/status.h"

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
    if (fd_ == -1) return absl::UnknownError("eventfd() failed");
    return absl::OkStatus();
  }

  void Close() {
    if (fd_ != -1) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  // Wake one waiter.
  inline void post() {
    const uint64_t one = 1;
    (void)!::write(fd_, &one, sizeof(one));  // best-effort; errors are fatal only if used
  }

  // Block until signaled.
  inline void wait() {
    uint64_t val;
    (void)!::read(fd_, &val, sizeof(val));
  }

  int fd() const { return fd_; }

 private:
  int fd_ = -1;
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H