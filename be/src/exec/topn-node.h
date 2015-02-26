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


#ifndef IMPALA_EXEC_TOPN_NODE_H
#define IMPALA_EXEC_TOPN_NODE_H

#include <queue>
#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
<<<<<<< HEAD
#include "runtime/descriptors.h"  // for TupleId
=======
#include "exec/sort-exec-exprs.h"
#include "runtime/descriptors.h"  // for TupleId
#include "util/tuple-row-compare.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

namespace impala {

class MemPool;
<<<<<<< HEAD
struct RuntimeState;
class Tuple;

// Node for in-memory TopN (ORDER BY ... LIMIT)
// This handles the case where the result fits in memory.  This node will do a deep
// copy of the tuples that are necessary for the output.
// This is implemented by storing rows in a priority queue.  
=======
class RuntimeState;
class Tuple;

// Node for in-memory TopN (ORDER BY ... LIMIT)
// This handles the case where the result fits in memory.
// This node will materialize its input rows into a new tuple using the expressions
// in sort_tuple_slot_exprs_ in its sort_exec_exprs_ member.
// TopN is implemented by storing rows in a priority queue.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class TopNNode : public ExecNode {
 public:
  TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

<<<<<<< HEAD
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);
=======
  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
<<<<<<< HEAD
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  class TupleRowLessThan {
   public:
    TupleRowLessThan() : node_(NULL) {}
    TupleRowLessThan(TopNNode* node) : node_(node) {}
    bool operator()(TupleRow* const& lhs, TupleRow* const& rhs) const;

   private:
    TopNNode* node_;
  };
    
  friend class TupleLessThan;

  // Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep 
=======

  friend class TupleLessThan;

  // Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // copy of tuple_row, which it stores in tuple_pool_.
  void InsertTupleRow(TupleRow* tuple_row);

  // Flatten and reverse the priority queue.
  void PrepareForOutput();

<<<<<<< HEAD
  std::vector<TupleDescriptor*> tuple_descs_;
  std::vector<bool> is_asc_order_;

  // Create two copies of the exprs for evaluating over the TupleRows.
  // The result of the evaluation is stored in the Expr, so it's not efficient to use
  // one set of Expr to compare TupleRows.
  std::vector<Expr*> lhs_ordering_exprs_;
  std::vector<Expr*> rhs_ordering_exprs_;

  TupleRowLessThan tuple_row_less_than_;

  // The priority queue will never have more elements in it than the LIMIT.  The stl 
  // priority queue doesn't support a max size, so to get that functionality, the order
  // of the queue is the opposite of what the ORDER BY clause specifies, such that the top 
  // of the queue is the last sorted element.
  std::priority_queue<TupleRow*, std::vector<TupleRow*>, TupleRowLessThan> priority_queue_;

  // After computing the TopN in the priority_queue, pop them and put them in this vector
  std::vector<TupleRow*> sorted_top_n_;
  std::vector<TupleRow*>::iterator get_next_iter_;
    
  // Stores everything referenced in priority_queue_
  boost::scoped_ptr<MemPool> tuple_pool_;
=======
  // Number of rows to skip.
  int64_t offset_;
  int64_t num_rows_skipped_;

  // sort_exec_exprs_ contains the ordering expressions used for tuple comparison and
  // the materialization exprs for the output tuple.
  SortExecExprs sort_exec_exprs_;
  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;
  // Cached descriptor for the materialized tuple. Assigned in Prepare().
  TupleDescriptor* materialized_tuple_desc_;

  boost::scoped_ptr<TupleRowComparator> tuple_row_less_than_;

  // The priority queue will never have more elements in it than the LIMIT + OFFSET.
  // The stl priority queue doesn't support a max size, so to get that functionality,
  // the order of the queue is the opposite of what the ORDER BY clause specifies, such
  // that the top of the queue is the last sorted element.
  boost::scoped_ptr<
      std::priority_queue<Tuple*, std::vector<Tuple*>, TupleRowComparator> >
          priority_queue_;

  // After computing the TopN in the priority_queue, pop them and put them in this vector
  std::vector<Tuple*> sorted_top_n_;
  std::vector<Tuple*>::iterator get_next_iter_;

  // Stores everything referenced in priority_queue_
  boost::scoped_ptr<MemPool> tuple_pool_;
  // Tuple allocated once from tuple_pool_ and reused in InsertTupleRow to
  // materialize input tuples if necessary. After materialization, tmp_tuple_ may be
  // copied into the the tuple pool and inserted into the priority queue.
  Tuple* tmp_tuple_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

};

#endif
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
