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

namespace cpp impala
namespace java com.cloudera.impala.thrift

enum TStatusCode {
  OK,
  CANCELLED,
  ANALYSIS_ERROR,
  NOT_IMPLEMENTED_ERROR,
  RUNTIME_ERROR,
<<<<<<< HEAD
  INTERNAL_ERROR
=======
  MEM_LIMIT_EXCEEDED,
  INTERNAL_ERROR,
  RECOVERABLE_ERROR
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

struct TStatus {
  1: required TStatusCode status_code
  2: list<string> error_msgs
}
