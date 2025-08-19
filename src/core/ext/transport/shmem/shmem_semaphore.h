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
#include <semaphore.h>
#include <fcntl.h>
#include <string>
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace grpc_shmem {

// Simple counting semaphore that supports both in-process (eventfd) and 
// cross-process (POSIX named semaphores) usage.
class EventFdSemaphore {
 public:
  EventFdSemaphore() = default;
  EventFdSemaphore(const EventFdSemaphore&) = delete;
  EventFdSemaphore& operator=(const EventFdSemaphore&) = delete;
  ~EventFdSemaphore() { Close(); }

  // Initialize for in-process usage (eventfd)
  absl::Status Init(unsigned initial = 0, bool semaphore_mode = true) {
    if (fd_ != -1 || posix_sem_ != SEM_FAILED) return absl::OkStatus();
    use_posix_ = false;
    int flags = EFD_CLOEXEC;
    if (semaphore_mode) flags |= EFD_SEMAPHORE;
    fd_ = ::eventfd(initial, flags);
    if (fd_ == -1) {
      int saved_errno = errno;
      LOG(ERROR) << "EventFdSemaphore::Init() eventfd failed: initial=" << initial 
                 << ", flags=0x" << std::hex << flags << std::dec 
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      return absl::UnknownError(absl::StrCat("eventfd() failed: ", strerror(saved_errno)));
    }
    return absl::OkStatus();
  }

  // Initialize for cross-process usage (named POSIX semaphore)
  absl::Status InitNamed(const std::string& name, unsigned initial = 0) {
    if (fd_ != -1 || posix_sem_ != SEM_FAILED) return absl::OkStatus();
    use_posix_ = true;
    sem_name_ = "/" + name;  // POSIX semaphore names must start with /
    
    // Try to create the semaphore first (server case)
    posix_sem_ = ::sem_open(sem_name_.c_str(), O_CREAT | O_EXCL, 0600, initial);
    if (posix_sem_ == SEM_FAILED && errno == EEXIST) {
      // Semaphore already exists, open it (client case)
      posix_sem_ = ::sem_open(sem_name_.c_str(), 0);
    }
    
    if (posix_sem_ == SEM_FAILED) {
      int saved_errno = errno;
      LOG(ERROR) << "EventFdSemaphore::InitNamed() sem_open failed: name=" << sem_name_
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      return absl::UnknownError(absl::StrCat("sem_open() failed: ", strerror(saved_errno)));
    }
    return absl::OkStatus();
  }

  void Close() {
    if (use_posix_) {
      if (posix_sem_ != SEM_FAILED) {
        int close_result = ::sem_close(posix_sem_);
        if (close_result != 0) {
          int saved_errno = errno;
          LOG(ERROR) << "EventFdSemaphore::Close() sem_close failed: name=" << sem_name_
                     << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
        }
        posix_sem_ = SEM_FAILED;
      }
    } else {
      if (fd_ != -1) {
        int close_result = ::close(fd_);
        if (close_result != 0) {
          int saved_errno = errno;
          LOG(ERROR) << "EventFdSemaphore::Close() failed: fd=" << fd_ 
                     << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
        }
        fd_ = -1;
      }
    }
  }

  // Wake one waiter.
  inline void post() {
    if (use_posix_) {
      int result = ::sem_post(posix_sem_);
      if (result != 0) {
        int saved_errno = errno;
        LOG(ERROR) << "EventFdSemaphore::post() sem_post failed: name=" << sem_name_
                   << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      }
    } else {
      const uint64_t one = 1;
      ssize_t result = ::write(fd_, &one, sizeof(one));
      if (result != sizeof(one)) {
        int saved_errno = errno;
        LOG(ERROR) << "EventFdSemaphore::post() write failed: fd=" << fd_ 
                   << ", result=" << result << ", errno=" << saved_errno 
                   << " (" << strerror(saved_errno) << ")";
      }
    }
  }

  // Block until signaled.
  inline void wait() {
    if (use_posix_) {
      int result = ::sem_wait(posix_sem_);
      if (result != 0) {
        int saved_errno = errno;
        if (saved_errno == EINTR) {
          // Interrupted by signal, retry
          LOG(INFO) << "EventFdSemaphore::wait() interrupted by signal, retrying";
          wait(); // Recursive retry
          return;
        }
        LOG(ERROR) << "EventFdSemaphore::wait() sem_wait failed: name=" << sem_name_
                   << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      }
    } else {
      uint64_t val;
      ssize_t result = ::read(fd_, &val, sizeof(val));
      if (result != sizeof(val)) {
        int saved_errno = errno;
        if (saved_errno == EINTR) {
          // Interrupted by signal, retry
          LOG(INFO) << "EventFdSemaphore::wait() interrupted by signal, retrying";
          wait(); // Recursive retry
          return;
        }
        LOG(ERROR) << "EventFdSemaphore::wait() read failed: fd=" << fd_ 
                   << ", result=" << result << ", errno=" << saved_errno 
                   << " (" << strerror(saved_errno) << ")";
      }
    }
  }

  int fd() const { return fd_; }
  
  // Static cleanup method for named semaphores
  static void UnlinkNamed(const std::string& name) {
    std::string sem_name = "/" + name;
    int result = ::sem_unlink(sem_name.c_str());
    if (result != 0 && errno != ENOENT) {
      int saved_errno = errno;
      LOG(WARNING) << "EventFdSemaphore::UnlinkNamed() failed: name=" << sem_name
                   << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
    }
  }

 private:
  int fd_ = -1;
  sem_t* posix_sem_ = SEM_FAILED;
  bool use_posix_ = false;
  std::string sem_name_;
};

// Cross-process semaphore manager that separates shared state from process handles
class CrossProcessSemaphore {
 public:
  CrossProcessSemaphore() = default;
  CrossProcessSemaphore(const CrossProcessSemaphore&) = delete;
  CrossProcessSemaphore& operator=(const CrossProcessSemaphore&) = delete;
  ~CrossProcessSemaphore() { Close(); }

  // Initialize from semaphore name stored in shared memory
  absl::Status InitFromName(const char* sem_name) {
    if (sem_name == nullptr || sem_name[0] == '\0') {
      return absl::InvalidArgumentError("Empty semaphore name");
    }
    
    // Open existing semaphore (both server and client case)
    posix_sem_ = ::sem_open(sem_name, 0);
    if (posix_sem_ == SEM_FAILED) {
      int saved_errno = errno;
      LOG(ERROR) << "CrossProcessSemaphore::InitFromName() sem_open failed: name=" << sem_name
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      return absl::UnknownError(absl::StrCat("sem_open() failed: ", strerror(saved_errno)));
    }
    
    sem_name_ = sem_name;
    return absl::OkStatus();
  }

  // Create and initialize semaphore with given name and initial value
  absl::Status CreateNamed(const std::string& name, unsigned initial = 0) {
    sem_name_ = "/" + name;  // POSIX semaphore names must start with /
    
    // Create the semaphore 
    posix_sem_ = ::sem_open(sem_name_.c_str(), O_CREAT | O_EXCL, 0600, initial);
    if (posix_sem_ == SEM_FAILED) {
      int saved_errno = errno;
      LOG(ERROR) << "CrossProcessSemaphore::CreateNamed() sem_open failed: name=" << sem_name_
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      return absl::UnknownError(absl::StrCat("sem_open() failed: ", strerror(saved_errno)));
    }
    return absl::OkStatus();
  }

  void Close() {
    if (posix_sem_ != SEM_FAILED) {
      int close_result = ::sem_close(posix_sem_);
      if (close_result != 0) {
        int saved_errno = errno;
        LOG(ERROR) << "CrossProcessSemaphore::Close() sem_close failed: name=" << sem_name_
                   << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      }
      posix_sem_ = SEM_FAILED;
    }
  }

  // Wake one waiter
  inline void post() {
    if (posix_sem_ == SEM_FAILED) {
      LOG(ERROR) << "CrossProcessSemaphore::post() called on uninitialized semaphore";
      return;
    }
    int result = ::sem_post(posix_sem_);
    if (result != 0) {
      int saved_errno = errno;
      LOG(ERROR) << "CrossProcessSemaphore::post() sem_post failed: name=" << sem_name_
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
    }
  }

  // Block until signaled
  inline void wait() {
    if (posix_sem_ == SEM_FAILED) {
      LOG(ERROR) << "CrossProcessSemaphore::wait() called on uninitialized semaphore";
      return;
    }
    
    while (true) {
      int result = ::sem_wait(posix_sem_);
      if (result == 0) break;  // Success
      
      int saved_errno = errno;
      if (saved_errno == EINTR) {
        // Interrupted by signal, retry
        continue;
      }
      LOG(ERROR) << "CrossProcessSemaphore::wait() sem_wait failed: name=" << sem_name_
                 << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
      break;
    }
  }

  // Get the semaphore name for storing in shared memory
  const std::string& name() const { return sem_name_; }

  // Static cleanup method
  static void UnlinkNamed(const std::string& name) {
    std::string sem_name = (name[0] == '/') ? name : "/" + name;
    int result = ::sem_unlink(sem_name.c_str());
    if (result != 0 && errno != ENOENT) {
      int saved_errno = errno;
      LOG(WARNING) << "CrossProcessSemaphore::UnlinkNamed() failed: name=" << sem_name
                   << ", errno=" << saved_errno << " (" << strerror(saved_errno) << ")";
    }
  }

 private:
  sem_t* posix_sem_ = SEM_FAILED;
  std::string sem_name_;
};

}  // namespace grpc_shmem

#endif  // GRPC_SRC_CORE_EXT_TRANSPORT_SHMEM_SHMEM_SEMAPHORE_H