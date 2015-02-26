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

#include "exec/scan-node.h"

#include <boost/bind.hpp>

using namespace std;
using namespace boost;

namespace impala {

<<<<<<< HEAD
const string ScanNode::BYTES_READ_COUNTER = "BytesRead";
const string ScanNode::READ_TIMER = "ScannerThreadsReadTime";
const string ScanNode::TOTAL_THROUGHPUT_COUNTER = "TotalReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime";
const string ScanNode::PER_THREAD_THROUGHPUT_COUNTER = "PerDiskReadThroughput";
const string ScanNode::SCAN_RANGES_COMPLETE_COUNTER = "ScanRangesComplete";

Status ScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TCounterType::BYTES);
  read_timer_ =
      ADD_COUNTER(runtime_profile(), READ_TIMER, TCounterType::CPU_TICKS);
  total_throughput_counter_ = runtime_profile()->AddRateCounter(
      TOTAL_THROUGHPUT_COUNTER, bytes_read_counter_);
  materialize_tuple_timer_ =
      ADD_COUNTER(runtime_profile(), MATERIALIZE_TUPLE_TIMER, TCounterType::CPU_TICKS);
  per_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
       PER_THREAD_THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
       bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TCounterType::UNIT);

=======
// Changing these names have compatibility concerns.
const string ScanNode::BYTES_READ_COUNTER = "BytesRead";
const string ScanNode::ROWS_READ_COUNTER = "RowsRead";
const string ScanNode::TOTAL_HDFS_READ_TIMER = "TotalRawHdfsReadTime(*)";
const string ScanNode::TOTAL_HBASE_READ_TIMER = "TotalRawHBaseReadTime(*)";
const string ScanNode::TOTAL_THROUGHPUT_COUNTER = "TotalReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime(*)";
const string ScanNode::PER_READ_THREAD_THROUGHPUT_COUNTER =
    "PerReadThreadRawHdfsThroughput";
const string ScanNode::NUM_DISKS_ACCESSED_COUNTER = "NumDisksAccessed";
const string ScanNode::SCAN_RANGES_COMPLETE_COUNTER = "ScanRangesComplete";
const string ScanNode::SCANNER_THREAD_COUNTERS_PREFIX = "ScannerThreads";
const string ScanNode::SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
const string ScanNode::AVERAGE_SCANNER_THREAD_CONCURRENCY =
    "AverageScannerThreadConcurrency";
const string ScanNode::AVERAGE_HDFS_READ_THREAD_CONCURRENCY =
    "AverageHdfsReadThreadConcurrency";
const string ScanNode::NUM_SCANNER_THREADS_STARTED =
    "NumScannerThreadsStarted";

Status ScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  scanner_thread_counters_ =
      ADD_THREAD_COUNTERS(runtime_profile(), SCANNER_THREAD_COUNTERS_PREFIX);
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TCounterType::BYTES);
  bytes_read_timeseries_counter_ = ADD_TIME_SERIES_COUNTER(runtime_profile(),
      BYTES_READ_COUNTER, bytes_read_counter_);
  rows_read_counter_ =
      ADD_COUNTER(runtime_profile(), ROWS_READ_COUNTER, TCounterType::UNIT);
  total_throughput_counter_ = runtime_profile()->AddRateCounter(
      TOTAL_THROUGHPUT_COUNTER, bytes_read_counter_);
  materialize_tuple_timer_ = ADD_CHILD_TIMER(runtime_profile(), MATERIALIZE_TUPLE_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

}
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
