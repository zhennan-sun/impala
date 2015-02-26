// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_STOPWATCH_H
#define IMPALA_UTIL_STOPWATCH_H

#include <boost/cstdint.hpp>
<<<<<<< HEAD
=======
#include <time.h>
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

namespace impala {

// Utility class to measure time.  This is measured using the cpu tick counter which
// is very low overhead but can be inaccurate if the thread is switched away.  This
// is useful for measuring cpu time at the row batch level (too much overhead at the
// row granularity).
class StopWatch {
 public:
  StopWatch() {
    total_time_ = 0;
    running_ = false;
  }

  void Start() {
    if (!running_) {
      start_ = Rdtsc();
      running_ = true;
    }
  }
<<<<<<< HEAD
  
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  void Stop() {
    if (running_) {
      total_time_ += Rdtsc() - start_;
      running_ = false;
    }
  }

  // Returns time in cpu ticks.
  uint64_t ElapsedTime() const {
    return running_ ? Rdtsc() - start_ : total_time_;
  }

  static uint64_t Rdtsc() {
    uint32_t lo, hi;
<<<<<<< HEAD
    __asm__ __volatile__ (      
=======
    __asm__ __volatile__ (
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      "xorl %%eax,%%eax \n        cpuid"
      ::: "%rax", "%rbx", "%rcx", "%rdx");
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
  }
<<<<<<< HEAD
 
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
 private:
  uint64_t start_, total_time_;
  bool running_;
};

<<<<<<< HEAD
// Stop watch for reporting elapsed time based on wall block.  This is heavyweight
// but accurate.
class WallClockStopWatch {
 public:
  WallClockStopWatch() {
=======
// Stop watch for reporting elapsed time in nanosec based on CLOCK_MONOTONIC.
// It is as fast as Rdtsc.
// It is also accurate because it not affected by cpu frequency changes and
// it is not affected by user setting the system clock.
// CLOCK_MONOTONIC represents monotonic time since some unspecified starting point.
// It is good for computing elapsed time.
class MonotonicStopWatch {
 public:
  MonotonicStopWatch() {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    total_time_ = 0;
    running_ = false;
  }

  void Start() {
    if (!running_) {
<<<<<<< HEAD
      start_ = GetTime();
      running_ = true;
    }
  }
  
  void Stop() {
    if (running_) {
      total_time_ += GetTime() - start_;
=======
      clock_gettime(CLOCK_MONOTONIC, &start_);
      running_ = true;
    }
  }

  void Stop() {
    if (running_) {
      total_time_ += ElapsedTime();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      running_ = false;
    }
  }

<<<<<<< HEAD
  // Returns time in ms.
  uint64_t ElapsedTime() {
    return running_ ? GetTime() - start_ : total_time_;
  }

 private:
  // Returns time (since epoch) in ms.
  static uint64_t GetTime();

  uint64_t start_, total_time_;
=======
  // Restarts the timer. Returns the elapsed time until this point.
  uint64_t Reset() {
    uint64_t ret = ElapsedTime();
    if (running_) {
      clock_gettime(CLOCK_MONOTONIC, &start_);
    }
    return ret;
  }

  // Returns time in nanosecond.
  uint64_t ElapsedTime() const {
    if (!running_) return total_time_;
    timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - start_.tv_sec) * 1000L * 1000L * 1000L +
        (end.tv_nsec - start_.tv_nsec);
  }

 private:
  timespec start_;
  uint64_t total_time_; // in nanosec
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  bool running_;
};

}

#endif
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
