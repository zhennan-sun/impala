#!/usr/bin/env bash
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

TARGET_BUILD_TYPE=Debug

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -codecoverage)
      TARGET_BUILD_TYPE=CODE_COVERAGE_DEBUG
      ;;
    -help)
      echo "make_debug.sh [-codecoverage]"
      echo "[-codecoverage] : build with 'gcov' code coverage instrumentation at the cost of performance"
      exit
      ;;
  esac
done

cd $IMPALA_HOME
bin/gen_build_version.py
cmake -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE .
make clean

rm -f $IMPALA_HOME/llvm-ir/impala-nosse.ll
rm -f $IMPALA_HOME/llvm-ir/impala-sse.ll

cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j4
=======
$IMPALA_HOME/bin/make_impala.sh -build_type=Debug -build_shared_libs $*
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
