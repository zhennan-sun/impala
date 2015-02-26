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

#include "exec/topn-node.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;

<<<<<<< HEAD
TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) 
  : ExecNode(pool, tnode, descs),
    tuple_row_less_than_(this),
    priority_queue_(tuple_row_less_than_),
    tuple_pool_(new MemPool) {
  // TODO: log errors in runtime state
  Status status = Init(pool, tnode);
  DCHECK(status.ok()) << "TopNNode c'tor:Init failed: \n" << status.GetErrorMsg();
}

Status TopNNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, tnode.sort_node.ordering_exprs, &lhs_ordering_exprs_));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, tnode.sort_node.ordering_exprs, &rhs_ordering_exprs_));
  is_asc_order_.insert(
      is_asc_order_.begin(), tnode.sort_node.is_asc_order.begin(),
      tnode.sort_node.is_asc_order.end());
  DCHECK_EQ(conjuncts_.size(), 0) << "TopNNode should never have predicates to evaluate.";
  return Status::OK;
}

// The stl::priority_queue is a MAX heap.
bool TopNNode::TupleRowLessThan::operator()(TupleRow* const& lhs, TupleRow* const& rhs)
    const {
  DCHECK(node_ != NULL);

  vector<Expr*>::const_iterator lhs_expr_iter = node_->lhs_ordering_exprs_.begin();
  vector<Expr*>::const_iterator rhs_expr_iter = node_->rhs_ordering_exprs_.begin();
  vector<bool>::const_iterator is_asc_iter = node_->is_asc_order_.begin();

  for (;lhs_expr_iter != node_->lhs_ordering_exprs_.end(); 
      ++lhs_expr_iter,++rhs_expr_iter,++is_asc_iter) {
    Expr* lhs_expr = *lhs_expr_iter;
    Expr* rhs_expr = *rhs_expr_iter;
    void *lhs_value = lhs_expr->GetValue(lhs);
    void *rhs_value = rhs_expr->GetValue(rhs);
    bool less_than = *is_asc_iter;

    // NULL's always go at the end regardless of asc/desc
    if (lhs_value == NULL && rhs_value == NULL) continue;
    if (lhs_value == NULL && rhs_value != NULL) return false;
    if (lhs_value != NULL && rhs_value == NULL) return true;

    int result = RawValue::Compare(lhs_value, rhs_value, lhs_expr->type());
    if (!less_than) {
      result = -result;
    }
    if (result > 0) return false;
    if (result < 0) return true;
    // Otherwise, try the next Expr
  }
  return true;
}

Status TopNNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  
  tuple_descs_ = child(0)->row_desc().tuple_descriptors();
  Expr::Prepare(lhs_ordering_exprs_, state, child(0)->row_desc());
  Expr::Prepare(rhs_ordering_exprs_, state, child(0)->row_desc());
=======
TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    num_rows_skipped_(0) {
}

Status TopNNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(sort_exec_exprs_.Init(tnode.sort_node.sort_info, pool_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;

  DCHECK_EQ(conjunct_ctxs_.size(), 0)
      << "TopNNode should never have predicates to evaluate.";

  return Status::OK;
}

Status TopNNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(mem_tracker()));
  RETURN_IF_ERROR(sort_exec_exprs_.Prepare(
      state, child(0)->row_desc(), row_descriptor_, expr_mem_tracker()));
  AddExprCtxsToFree(sort_exec_exprs_);
  materialized_tuple_desc_ = row_descriptor_.tuple_descriptors()[0];
  // Allocate memory for a temporary tuple.
  tmp_tuple_ = reinterpret_cast<Tuple*>(
      tuple_pool_->Allocate(materialized_tuple_desc_->byte_size()));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

Status TopNNode::Open(RuntimeState* state) {
<<<<<<< HEAD
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(child(0)->Open(state));

  RowBatch batch(child(0)->row_desc(), state->batch_size());
  bool eos;
  do {
    RETURN_IF_CANCELLED(state);
    batch.Reset();
    RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
    for (int i = 0; i < batch.num_rows(); ++i) {
      InsertTupleRow(batch.GetRow(i));
    }
  } while (!eos);
  
  DCHECK_LE(priority_queue_.size(), limit_);
  PrepareForOutput();
=======
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(sort_exec_exprs_.Open(state));

  tuple_row_less_than_.reset(new TupleRowComparator(
      sort_exec_exprs_.lhs_ordering_expr_ctxs(), sort_exec_exprs_.rhs_ordering_expr_ctxs(),
      is_asc_order_, nulls_first_));
  priority_queue_.reset(
      new priority_queue<Tuple*, vector<Tuple*>, TupleRowComparator>(
          *tuple_row_less_than_));

  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (limit_ != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      for (int i = 0; i < batch.num_rows(); ++i) {
        InsertTupleRow(batch.GetRow(i));
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
  }
  DCHECK_LE(priority_queue_->size(), limit_ + offset_);
  PrepareForOutput();
  child(0)->Close(state);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
<<<<<<< HEAD
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  while (!row_batch->IsFull() && (get_next_iter_ != sorted_top_n_.end())) {
    int row_idx = row_batch->AddRow();
    TupleRow* dst_row = row_batch->GetRow(row_idx);
    TupleRow* src_row = *get_next_iter_;
=======
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  while (!row_batch->AtCapacity() && (get_next_iter_ != sorted_top_n_.end())) {
    if (num_rows_skipped_ < offset_) {
      ++get_next_iter_;
      ++num_rows_skipped_;
      continue;
    }
    int row_idx = row_batch->AddRow();
    TupleRow* dst_row = row_batch->GetRow(row_idx);
    Tuple* src_tuple = *get_next_iter_;
    TupleRow* src_row = reinterpret_cast<TupleRow*>(&src_tuple);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    row_batch->CopyRow(src_row, dst_row);
    ++get_next_iter_;
    row_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  *eos = get_next_iter_ == sorted_top_n_.end();
<<<<<<< HEAD
  return Status::OK; 
}

Status TopNNode::Close(RuntimeState* state) {
  COUNTER_UPDATE(memory_used_counter(), tuple_pool_->peak_allocated_bytes());
  return ExecNode::Close(state);
=======
  return Status::OK;
}

void TopNNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  sort_exec_exprs_.Close(state);
  ExecNode::Close(state);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

// Insert if either not at the limit or it's a new TopN tuple_row
void TopNNode::InsertTupleRow(TupleRow* input_row) {
<<<<<<< HEAD
  TupleRow* insert_tuple_row = NULL;
  
  if (priority_queue_.size() < limit_) {
    insert_tuple_row = input_row->DeepCopy(tuple_descs_, tuple_pool_.get());
  } else {
    TupleRow* top_tuple_row = priority_queue_.top();
    if (tuple_row_less_than_(input_row, top_tuple_row)) {
      for (int i = 0; i < tuple_descs_.size(); ++i) {
        Tuple* dst_tuple = top_tuple_row->GetTuple(i);
        Tuple* src_tuple = input_row->GetTuple(i);
        // TODO: DeepCopy will allocate new buffers for the string data.  This needs
        // to be fixed to use a freelist
        src_tuple->DeepCopy(dst_tuple, *(tuple_descs_[i]), tuple_pool_.get());
      }
      insert_tuple_row = top_tuple_row;
      priority_queue_.pop();
    }
  }

  if (insert_tuple_row != NULL) {
    priority_queue_.push(insert_tuple_row);
  }
=======
  Tuple* insert_tuple = NULL;

  if (priority_queue_->size() < limit_ + offset_) {
    insert_tuple = reinterpret_cast<Tuple*>(
        tuple_pool_->Allocate(materialized_tuple_desc_->byte_size()));
    insert_tuple->MaterializeExprs<false>(input_row, *materialized_tuple_desc_,
        sort_exec_exprs_.sort_tuple_slot_expr_ctxs(), tuple_pool_.get());
  } else {
    DCHECK(!priority_queue_->empty());
    Tuple* top_tuple = priority_queue_->top();
    tmp_tuple_->MaterializeExprs<false>(input_row, *materialized_tuple_desc_,
            sort_exec_exprs_.sort_tuple_slot_expr_ctxs(), NULL);
    if ((*tuple_row_less_than_)(tmp_tuple_, top_tuple)) {
      // TODO: DeepCopy() will allocate new buffers for the string data. This needs
      // to be fixed to use a freelist
      tmp_tuple_->DeepCopy(top_tuple, *materialized_tuple_desc_, tuple_pool_.get());
      insert_tuple = top_tuple;
      priority_queue_->pop();
    }
  }

  if (insert_tuple != NULL) priority_queue_->push(insert_tuple);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

// Reverse the order of the tuples in the priority queue
void TopNNode::PrepareForOutput() {
<<<<<<< HEAD
  sorted_top_n_.resize(priority_queue_.size());
  int index = sorted_top_n_.size() - 1;

  while (priority_queue_.size() > 0) {
    TupleRow* tuple_row = priority_queue_.top();
    priority_queue_.pop();
    sorted_top_n_[index] = tuple_row;
=======
  sorted_top_n_.resize(priority_queue_->size());
  int index = sorted_top_n_.size() - 1;

  while (priority_queue_->size() > 0) {
    Tuple* tuple = priority_queue_->top();
    priority_queue_->pop();
    sorted_top_n_[index] = tuple;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    --index;
  }

  get_next_iter_ = sorted_top_n_.begin();
}

void TopNNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TopNNode("
<<<<<<< HEAD
       << " ordering_exprs=" << Expr::DebugString(lhs_ordering_exprs_)
       << " sort_order=[";
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "") << (is_asc_order_[i] ? "asc" : "desc");
  }
  *out << "]";
=======
      << Expr::DebugString(sort_exec_exprs_.lhs_ordering_expr_ctxs());
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "")
         << (is_asc_order_[i] ? "asc" : "desc")
         << " nulls " << (nulls_first_[i] ? "first" : "last");
 }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
