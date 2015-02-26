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


cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

<<<<<<< HEAD
add_library(Exec STATIC
  aggregation-node.cc
  aggregation-node-ir.cc
  buffered-byte-stream.cc
  data-sink.cc
  ddl-executor.cc
  delimited-text-parser.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hash-join-node-ir.cc
  hash-table.cc
  hdfs-byte-stream.cc
=======
add_library(Exec
  aggregation-node.cc
  aggregation-node-ir.cc
  analytic-eval-node.cc
  base-sequence-scanner.cc
  blocking-join-node.cc
  catalog-op-executor.cc
  cross-join-node.cc
  data-sink.cc
  data-source-scan-node.cc
  delimited-text-parser.cc
  empty-set-node.cc
  exec-node.cc
  exchange-node.cc
  external-data-source-executor.cc
  hash-join-node.cc
  hash-join-node-ir.cc
  old-hash-table.cc
  hash-table.cc
  hbase-table-sink.cc
  hbase-table-writer.cc
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  hdfs-scan-node.cc
  hdfs-scanner.cc
  hdfs-scanner-ir.cc
  hdfs-table-sink.cc
  hdfs-table-writer.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
<<<<<<< HEAD
  hdfs-text-scanner.cc
  hdfs-text-table-writer.cc
  merge-node.cc
  hdfs-trevni-scanner.cc
  hdfs-trevni-table-writer.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  read-write-util.cc
  scan-range-context.cc
  serde-utils.cc
  scan-node.cc
  text-converter.cc
  topn-node.cc
)

target_link_libraries(Exec
  Runtime
  ${JAVA_JVM_LIBRARY}
  ${HDFS_LIBS}
)

#add_executable(sequence-file-recovery-test sequence-file-recovery-test.cc)
#target_link_libraries(sequence-file-recovery-test ${IMPALA_TEST_LINK_LIBS})
#add_test(sequence-file-recovery-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/sequence-file-recovery-test)

add_executable(zigzag-test zigzag-test.cc)
target_link_libraries(zigzag-test ${IMPALA_TEST_LINK_LIBS})
add_test(zigzag-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/zigzag-test)

add_executable(hash-table-test hash-table-test.cc)
target_link_libraries(hash-table-test ${IMPALA_TEST_LINK_LIBS})
add_test(hash-table-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/hash-table-test)

add_executable(delimited-text-parser-test delimited-text-parser-test.cc)
target_link_libraries(delimited-text-parser-test ${IMPALA_TEST_LINK_LIBS})
add_test(delimited-text-parser-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/delimited-text-parser-test)
=======
  hdfs-avro-scanner.cc
  hdfs-avro-table-writer.cc
  hdfs-avro-scanner-ir.cc
  hdfs-text-scanner.cc
  hdfs-lzo-text-scanner.cc
  hdfs-text-table-writer.cc
  hdfs-sequence-table-writer.cc
  hdfs-parquet-scanner.cc
  hdfs-parquet-table-writer.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  incr-stats-util.cc
  partitioned-aggregation-node.cc
  partitioned-aggregation-node-ir.cc
  partitioned-hash-join-node.cc
  partitioned-hash-join-node-ir.cc
  read-write-util.cc
  scan-node.cc
  scanner-context.cc
  select-node.cc
  sort-exec-exprs.cc
  sort-node.cc
  text-converter.cc
  topn-node.cc
  union-node.cc
)

ADD_BE_TEST(zigzag-test)
ADD_BE_TEST(old-hash-table-test)
ADD_BE_TEST(hash-table-test)
ADD_BE_TEST(delimited-text-parser-test)
ADD_BE_TEST(read-write-util-test)
ADD_BE_TEST(parquet-plain-test)
ADD_BE_TEST(parquet-version-test)
ADD_BE_TEST(row-batch-list-test)
ADD_BE_TEST(incr-stats-util-test)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
