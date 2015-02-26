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


#ifndef IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

<<<<<<< HEAD
#include "testutil/query-executor-if.h"
#include "util/thrift-client.h"
#include "common/status.h"
#include "exec/exec-stats.h"
#include "runtime/primitive-type.h"
#include "runtime/runtime-state.h"
=======
#include "rpc/thrift-client.h"
#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/types.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"

namespace impala {

<<<<<<< HEAD
// Query execution against running impalad process.
class ImpaladQueryExecutor : public QueryExecutorIf {
=======
class RowBatch;

// Query execution against running impalad process.
class ImpaladQueryExecutor {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
 public:
  ImpaladQueryExecutor();
  ~ImpaladQueryExecutor();

<<<<<<< HEAD
  virtual Status Setup();

  // Start running query. Call this prior to FetchResult().
  // If 'col_types' is non-NULL, returns the types of the select list items.
  virtual Status Exec(
      const std::string& query_string, std::vector<PrimitiveType>* col_types);

  // Return the explain plan for the query
  virtual Status Explain(const std::string& query_string, std::string* explain_plan);
=======
  Status Setup();

  // Start running query. Call this prior to FetchResult().
  // If 'col_types' is non-NULL, returns the types of the select list items.
  Status Exec(const std::string& query_string,
      std::vector<Apache::Hadoop::Hive::FieldSchema>* col_types);

  // Return the explain plan for the query
  Status Explain(const std::string& query_string, std::string* explain_plan);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Returns result batch in 'batch'. The returned rows are the output rows of
  // the execution tree. In other words, they do *not* reflect the query's
  // select list exprs, ie, don't call this if the query
  // doesn't have a FROM clause, this function will not return any result rows for
  // that case.
  // Sets 'batch' to NULL if no more data. Batch is owned by ImpaladQueryExecutor
  // and must not be deallocated.
<<<<<<< HEAD
  virtual Status FetchResult(RowBatch** batch);
=======
  Status FetchResult(RowBatch** batch);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Return single row as comma-separated list of values.
  // Indicates end-of-stream by setting 'row' to the empty string.
  // Returns OK if successful, otherwise error.
<<<<<<< HEAD
  virtual Status FetchResult(std::string* row);
=======
  Status FetchResult(std::string* row);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Return single row as vector of raw values.
  // Indicates end-of-stream by returning empty 'row'.
  // Returns OK if successful, otherwise error.
<<<<<<< HEAD
  virtual Status FetchResult(std::vector<void*>* row);

  // Returns the error log lines in executor_'s runtime state as a string joined
  // with '\n'.
  virtual std::string ErrorString() const;

  // Returns a string representation of the file_errors_.
  virtual std::string FileErrors() const;

  // Returns the counters for the entire query
  virtual RuntimeProfile* query_profile();

  virtual bool eos() { return eos_; }

  virtual ExecStats* exec_stats() { return &exec_stats_; }
=======
  Status FetchResult(std::vector<void*>* row);

  // Returns the error log lines in executor_'s runtime state as a string joined
  // with '\n'.
  std::string ErrorString() const;

  // Returns a string representation of the file_errors_.
  std::string FileErrors() const;

  // Returns the counters for the entire query
  RuntimeProfile* query_profile();

  bool eos() { return eos_; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  void setExecOptions(const std::vector<std::string>& exec_options) {
    exec_options_ = exec_options;
  }

 private:
  // fe service-related
  boost::scoped_ptr<ThriftClient<ImpalaServiceClient> > client_;

  // Execution options
  std::vector<std::string> exec_options_;

  // Beeswax query handle and result
  beeswax::QueryHandle query_handle_;
  beeswax::Results query_results_;
  beeswax::QueryExplanation query_explanation_;

  bool query_in_progress_;
  int current_row_;
  bool eos_;
<<<<<<< HEAD
  ExecStats exec_stats_;  // not set right now
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // call beeswax.close() for current query, if one in progress
  Status Close();
};

}

#endif
