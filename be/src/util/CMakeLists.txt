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

<<<<<<< HEAD
=======
# Without this option Squeasel looks up the SSL library at run-time
# and may not guess the correct name on some distributions
SET_SOURCE_FILES_PROPERTIES(${SQUEASEL_SRC_DIR}/squeasel.c PROPERTIES
  COMPILE_FLAGS -DNO_SSL_DL)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
<<<<<<< HEAD
  authorization.cc
  benchmark.cc
  codec.cc
  compress.cc
  cpu-info.cc
=======
  benchmark.cc
  bitmap.cc
  cgroups-mgr.cc
  codec.cc
  compress.cc
  cpu-info.cc
  decimal-util.cc
  dynamic-util.cc
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  debug-util.cc
  decompress.cc
  default-path-handlers.cc
  disk-info.cc
<<<<<<< HEAD
  hdfs-util.cc
  integer-array.cc
  jni-util.cc
  logging.cc
  metrics.cc
  path-builder.cc
  perf-counters.cc
  progress-updater.cc
  runtime-profile.cc
  thrift-util.cc
  thrift-client.cc
  thrift-server.cc
  static-asserts.cc
  stopwatch.cc
  url-parser.cc
  )

add_library(Webserver STATIC
  ${MONGOOSE_SRC_DIR}/mongoose.c
  webserver.cc
)

target_link_libraries(Webserver pthread dl)

add_executable(integer-array-test integer-array-test.cc)
add_executable(perf-counters-test perf-counters-test.cc)
add_executable(runtime-profile-test runtime-profile-test.cc)
add_executable(benchmark-test benchmark-test.cc)
add_executable(decompress-test decompress-test.cc)
add_executable(metrics-test metrics-test.cc)
add_executable(debug-util-test debug-util-test.cc)
add_executable(refresh-catalog refresh-catalog.cc)

target_link_libraries(integer-array-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(perf-counters-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(runtime-profile-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(benchmark-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(decompress-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(metrics-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(debug-util-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(refresh-catalog ${IMPALA_LINK_LIBS})

add_test(integer-array-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/integer-array-test)
add_test(perf-counters-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/perf-counters-test)
add_test(runtime-profile-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/runtime-profile-test)
add_test(benchmark-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/benchmark-test)
add_test(decompress-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/decompress-test)
add_test(metrics-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/metrics-test)
add_test(debug-util-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/debug-util-test)

=======
  error-util.cc
  filesystem-util.cc
  hdfs-util.cc
  hdfs-bulk-ops.cc
  impalad-metrics.cc
  jni-util.cc
  llama-util.cc
  logging-support.cc
  mem-info.cc
  memory-metrics.cc
  metrics.cc
  network-util.cc
  os-info.cc
  os-util.cc
  parse-util.cc
  path-builder.cc
  periodic-counter-updater
  pprof-path-handlers.cc
# TODO: not supported on RHEL 5
#  perf-counters.cc
  progress-updater.cc
  runtime-profile.cc
  simple-logger.cc
  symbols-util.cc
  static-asserts.cc
  summary-util.cc
  table-printer.cc
  test-info.cc
  thread.cc
  time.cc
  url-parser.cc
  url-coding.cc
)

add_library(Webserver STATIC
  ${SQUEASEL_SRC_DIR}/squeasel.c
  webserver.cc
  ${MUSTACHE_SRC_DIR}/mustache.cc
)

# Squeasel requires C99 compatibility to build.
SET_SOURCE_FILES_PROPERTIES(${SQUEASEL_SRC_DIR}/squeasel.c
  PROPERTIES COMPILE_FLAGS -std=c99)

# shared library which provides native logging support to JVMs over JNI.
add_library(loggingsupport SHARED
  logging-support.cc
)

target_link_libraries(Webserver pthread dl Util ssl crypto)

add_executable(network-perf-benchmark network-perf-benchmark.cc)
add_executable(parquet-reader parquet-reader.cc)

target_link_libraries(network-perf-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(parquet-reader ${IMPALA_LINK_LIBS})
target_link_libraries(loggingsupport ${IMPALA_LINK_LIBS_NO_TCMALLOC})

ADD_BE_TEST(runtime-profile-test)
ADD_BE_TEST(benchmark-test)
ADD_BE_TEST(decompress-test)
ADD_BE_TEST(metrics-test)
ADD_BE_TEST(debug-util-test)
ADD_BE_TEST(url-coding-test)
ADD_BE_TEST(bit-util-test)
ADD_BE_TEST(rle-test)
ADD_BE_TEST(blocking-queue-test)
ADD_BE_TEST(dict-test)
ADD_BE_TEST(thread-pool-test)
ADD_BE_TEST(internal-queue-test)
ADD_BE_TEST(string-parser-test)
ADD_BE_TEST(promise-test)
ADD_BE_TEST(symbols-util-test)
#ADD_BE_TEST(perf-counters-test)
ADD_BE_TEST(webserver-test)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
