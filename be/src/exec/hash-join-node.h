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


#ifndef IMPALA_EXEC_HASH_JOIN_NODE_H
#define IMPALA_EXEC_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
<<<<<<< HEAD
#include <boost/unordered_set.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"
=======
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/old-hash-table.h"
#include "exec/blocking-join-node.h"
#include "util/promise.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class MemPool;
class RowBatch;
class TupleRow;

// Node for in-memory hash joins:
// - builds up a hash table with the rows produced by our right input
//   (child(1)); build exprs are the rhs exprs of our equi-join predicates
// - for each row from our left input, probes the hash table to retrieve
//   matching entries; the probe exprs are the lhs exprs of our equi-join predicates
//
// Row batches:
// - In general, we are not able to pass our output row batch on to our left child (when
//   we're fetching the probe rows): if we have a 1xn join, our output will contain
//   multiple rows per left input row
// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
//   we don't do these extra copies
<<<<<<< HEAD
class HashJoinNode : public ExecNode {
 public:
  HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~HashJoinNode();

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);
=======
class HashJoinNode : public BlockingJoinNode {
 public:
  HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  // Open() implemented in BlockingJoinNode
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  static const char* LLVM_CLASS_NAME;

 protected:
<<<<<<< HEAD
  void DebugString(int indentation_level, std::stringstream* out) const;
  
 private:
  boost::scoped_ptr<HashTable> hash_tbl_;
  HashTable::Iterator hash_tbl_iterator_;

  // for right outer joins, keep track of what's been joined
  typedef boost::unordered_set<TupleRow*> BuildTupleRowSet;
  BuildTupleRowSet joined_build_rows_;

  TJoinOp::type join_op_;

  // our equi-join predicates "<lhs> = <rhs>" are separated into
  // build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
  std::vector<Expr*> probe_exprs_;
  std::vector<Expr*> build_exprs_;

  // non-equi-join conjuncts from the JOIN clause
  std::vector<Expr*> other_join_conjuncts_;

  // derived from join_op_
  bool match_all_probe_;  // output all rows coming from the probe input
  bool match_one_build_;  // match at most one build row to each probe row
  bool match_all_build_;  // output all rows coming from the build input

  bool matched_probe_;  // if true, we have matched the current probe row
  bool eos_;  // if true, nothing left to return in GetNext()
  boost::scoped_ptr<MemPool> build_pool_;  // holds everything referenced in hash_tbl_

  // probe_batch_ must be cleared before calling GetNext().  The child node
  // does not initialize all tuple ptrs in the row, only the ones that it
  // is responsible for.
  boost::scoped_ptr<RowBatch> probe_batch_;
  int probe_batch_pos_;  // current scan pos in probe_batch_
  bool probe_eos_;  // if true, probe child has no more rows to process
  TupleRow* current_probe_row_;

  // build_tuple_idx_[i] is the tuple index of child(1)'s tuple[i] in the output row
  std::vector<int> build_tuple_idx_;
  int build_tuple_size_;

  // byte size of result tuple row (sum of the tuple ptrs, not the tuple data).  
  // This should be the same size as the probe tuple row.
  int result_tuple_row_size_;
  
=======
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const;
  virtual Status InitGetNext(TupleRow* first_probe_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  boost::scoped_ptr<OldHashTable> hash_tbl_;
  OldHashTable::Iterator hash_tbl_iterator_;

  // our equi-join predicates "<lhs> = <rhs>" are separated into
  // build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
    std::vector<ExprContext*> probe_expr_ctxs_;
    std::vector<ExprContext*> build_expr_ctxs_;

  // non-equi-join conjuncts from the JOIN clause
  std::vector<ExprContext*> other_join_conjunct_ctxs_;

  // Derived from join_op_
  // Output all rows coming from the probe input. Used in LEFT_OUTER_JOIN and
  // FULL_OUTER_JOIN.
  bool match_all_probe_;

  // Match at most one build row to each probe row. Used in LEFT_SEMI_JOIN.
  bool match_one_build_;

  // Output all rows coming from the build input. Used in RIGHT_OUTER_JOIN and
  // FULL_OUTER_JOIN.
  bool match_all_build_;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // llvm function for build batch
  llvm::Function* codegen_process_build_batch_fn_;

  // Function declaration for codegen'd function.  Signature must match
  // HashJoinNode::ProcessBuildBatch
  typedef void (*ProcessBuildBatchFn)(HashJoinNode*, RowBatch*);
  ProcessBuildBatchFn process_build_batch_fn_;
<<<<<<< HEAD
  
  // llvm function object for probe batch
  llvm::Function* codegen_process_probe_batch_fn_;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // HashJoinNode::ProcessProbeBatch() exactly
  typedef int (*ProcessProbeBatchFn)(HashJoinNode*, RowBatch*, RowBatch*, int);
  // Jitted ProcessProbeBatch function pointer.  Null if codegen is disabled.
  ProcessProbeBatchFn process_probe_batch_fn_;
<<<<<<< HEAD
  
  RuntimeProfile::Counter* build_timer_;   // time to build hash table
  RuntimeProfile::Counter* probe_timer_;   // time to probe
  RuntimeProfile::Counter* build_row_counter_;   // num build rows
  RuntimeProfile::Counter* probe_row_counter_;   // num probe rows
  RuntimeProfile::Counter* build_buckets_counter_;   // num buckets in hash table

  // set up build_- and probe_exprs_
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  // GetNext helper function for the common join cases: Inner join, left semi and left 
=======

  RuntimeProfile::Counter* build_buckets_counter_;   // num buckets in hash table
  RuntimeProfile::Counter* hash_tbl_load_factor_counter_;

  // GetNext helper function for the common join cases: Inner join, left semi and left
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // outer
  Status LeftJoinGetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Processes a probe batch for the common (non right-outer join) cases.
  //  out_batch: the batch for resulting tuple rows
  //  probe_batch: the probe batch to process.  This function can be called to
  //    continue processing a batch in the middle
  //  max_added_rows: maximum rows that can be added to out_batch
  // return the number of rows added to out_batch
<<<<<<< HEAD
  int ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows); 
=======
  int ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Construct the build hash table, adding all the rows in 'build_batch'
  void ProcessBuildBatch(RowBatch* build_batch);

<<<<<<< HEAD
  // Write combined row, consisting of probe_row and build_row, to out_row.
  // This is replaced by codegen.
  void CreateOutputRow(TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row);

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Codegen function to create output row
  llvm::Function* CodegenCreateOutputRow(LlvmCodeGen* codegen);

  // Codegen processing build batches.  Identical signature to ProcessBuildBatch.
  // hash_fn is the codegen'd function for computing hashes over tuple rows in the
  // hash table.
  // Returns NULL if codegen was not possible.
<<<<<<< HEAD
  llvm::Function* CodegenProcessBuildBatch(LlvmCodeGen*, llvm::Function* hash_fn);
  
=======
  llvm::Function* CodegenProcessBuildBatch(RuntimeState* state, llvm::Function* hash_fn);

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Codegen processing probe batches.  Identical signature to ProcessProbeBatch.
  // hash_fn is the codegen'd function for computing hashes over tuple rows in the
  // hash table.
  // Returns NULL if codegen was not possible.
<<<<<<< HEAD
  llvm::Function* CodegenProcessProbeBatch(LlvmCodeGen*, llvm::Function* hash_fn);
=======
  llvm::Function* CodegenProcessProbeBatch(RuntimeState* state, llvm::Function* hash_fn);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif
