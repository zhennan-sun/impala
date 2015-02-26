# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

<<<<<<< HEAD
add_library(Runtime STATIC
=======
add_library(Runtime
  buffered-block-mgr.cc
  buffered-tuple-stream.cc
  buffered-tuple-stream-ir.cc
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  client-cache.cc
  coordinator.cc
  data-stream-mgr.cc
  data-stream-sender.cc
<<<<<<< HEAD
  descriptors.cc
  disk-io-mgr.cc
  disk-io-mgr-stress.cc
  exec-env.cc
  hbase-table-cache.cc
  hdfs-fs-cache.cc
  mem-pool.cc
  parallel-executor.cc
  plan-fragment-executor.cc
  primitive-type.cc
  raw-value.cc
  row-batch.cc
  runtime-state.cc
  string-value.cc
  timestamp-value.cc
  tuple.cc
  tuple-row.cc
)

add_executable(mem-pool-test mem-pool-test.cc)
add_executable(free-list-test  free-list-test.cc)
add_executable(string-buffer-test  string-buffer-test.cc)
add_executable(data-stream-test data-stream-test.cc)
add_executable(timestamp-test timestamp-test.cc)
add_executable(disk-io-mgr-test disk-io-mgr-test.cc)
add_executable(disk-io-mgr-stress-test disk-io-mgr-stress-test.cc)
add_executable(parallel-executor-test parallel-executor-test.cc)

target_link_libraries(mem-pool-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(free-list-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(string-buffer-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(data-stream-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(timestamp-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(disk-io-mgr-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(disk-io-mgr-stress-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(parallel-executor-test ${IMPALA_TEST_LINK_LIBS})

add_test(mem-pool-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/mem-pool-test)
add_test(free-list-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/free-list-test)
add_test(string-buffer-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/string-buffer-test)
add_test(data-stream-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/data-stream-test)
add_test(timestamp-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/timestamp-test)
add_test(disk-io-mgr-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/disk-io-mgr-test)
add_test(parallel-executor-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/parallel-executor-test)
=======
  data-stream-recvr.cc
  descriptors.cc
  disk-io-mgr.cc
  disk-io-mgr-reader-context.cc
  disk-io-mgr-scan-range.cc
  disk-io-mgr-stress.cc
  exec-env.cc
  hbase-table.cc
  hbase-table-factory.cc
  hdfs-fs-cache.cc
  lib-cache.cc
  mem-tracker.cc
  mem-pool.cc
  multi-precision.cc
  parallel-executor.cc
  plan-fragment-executor.cc
  types.cc
  raw-value.cc
  row-batch.cc
  runtime-state.cc
  sorted-run-merger.cc
  sorter.cc
  string-value.cc
  thread-resource-mgr.cc
  timestamp-parse-util.cc
  timestamp-value.cc
  tuple.cc
  tuple-row.cc
  tmp-file-mgr.cc
)

# This test runs forever so should not be part of 'make test'
add_executable(disk-io-mgr-stress-test disk-io-mgr-stress-test.cc)
target_link_libraries(disk-io-mgr-stress-test ${IMPALA_TEST_LINK_LIBS})

ADD_BE_TEST(mem-pool-test)
ADD_BE_TEST(free-pool-test)
ADD_BE_TEST(string-buffer-test)
ADD_BE_TEST(data-stream-test)
ADD_BE_TEST(timestamp-test)
ADD_BE_TEST(disk-io-mgr-test)
ADD_BE_TEST(buffered-block-mgr-test)
ADD_BE_TEST(parallel-executor-test)
ADD_BE_TEST(raw-value-test)
ADD_BE_TEST(string-value-test)
ADD_BE_TEST(thread-resource-mgr-test)
ADD_BE_TEST(mem-tracker-test)
ADD_BE_TEST(multi-precision-test)
ADD_BE_TEST(decimal-test)
ADD_BE_TEST(buffered-tuple-stream-test)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
