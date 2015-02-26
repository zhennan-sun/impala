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

#include "codegen/impala-ir.h"
#include "exec/hash-join-node.h"
<<<<<<< HEAD
#include "exec/hash-table.inline.h"
=======
#include "exec/old-hash-table.inline.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/row-batch.h"

using namespace std;
using namespace impala;

<<<<<<< HEAD
// Functions in this file are cross compiled to IR with clang.  
=======
// Functions in this file are cross compiled to IR with clang.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
<<<<<<< HEAD
// we will not be able to replace the funcitons with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts(Expr* const* exprs, int num_exprs,
    TupleRow* row) {
  return ExecNode::EvalConjuncts(exprs, num_exprs, row);
=======
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts2(
    ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  return ExecNode::EvalConjuncts(ctxs, num_ctxs, row);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
<<<<<<< HEAD
int HashJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, 
=======
int HashJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    int max_added_rows) {
  // This path does not handle full outer or right outer joins
  DCHECK(!match_all_build_);

  int row_idx = out_batch->AddRows(max_added_rows);
  DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
  uint8_t* out_row_mem = reinterpret_cast<uint8_t*>(out_batch->GetRow(row_idx));
  TupleRow* out_row = reinterpret_cast<TupleRow*>(out_row_mem);

  int rows_returned = 0;
  int probe_rows = probe_batch->num_rows();

<<<<<<< HEAD
  Expr* const* other_conjuncts = &other_join_conjuncts_[0];
  int num_other_conjuncts = other_join_conjuncts_.size();

  Expr* const* conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

  while (true) {
    // Create output row for each matching build row
    while (hash_tbl_iterator_.HasNext()) {
=======
  ExprContext* const* other_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_other_conjunct_ctxs = other_join_conjunct_ctxs_.size();

  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  int num_conjunct_ctxs = conjunct_ctxs_.size();

  while (true) {
    // Create output row for each matching build row
    while (!hash_tbl_iterator_.AtEnd()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
      hash_tbl_iterator_.Next<true>();
      CreateOutputRow(out_row, current_probe_row_, matched_build_row);

<<<<<<< HEAD
      if (!EvalOtherJoinConjuncts(other_conjuncts, num_other_conjuncts, out_row)) {
=======
      if (!EvalOtherJoinConjuncts2(
              other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        continue;
      }

      matched_probe_ = true;

<<<<<<< HEAD
      if (EvalConjuncts(conjuncts, num_conjuncts, out_row)) {
=======
      if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        ++rows_returned;
        // Filled up out batch or hit limit
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }

      // Handle left semi-join
      if (match_one_build_) {
        hash_tbl_iterator_ = hash_tbl_->End();
        break;
      }
    }

    // Handle left outer-join
    if (!matched_probe_ && match_all_probe_) {
      CreateOutputRow(out_row, current_probe_row_, NULL);
      matched_probe_ = true;
<<<<<<< HEAD
      if (EvalConjuncts(conjuncts, num_conjuncts, out_row)) {
=======
      if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        ++rows_returned;
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }
    }
<<<<<<< HEAD
    
    if (!hash_tbl_iterator_.HasNext()) {
=======

    if (hash_tbl_iterator_.AtEnd()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      // Advance to the next probe row
      if (UNLIKELY(probe_batch_pos_ == probe_rows)) goto end;
      current_probe_row_ = probe_batch->GetRow(probe_batch_pos_++);
      hash_tbl_iterator_ = hash_tbl_->Find(current_probe_row_);
      matched_probe_ = false;
    }
  }

end:
  if (match_one_build_ && matched_probe_) hash_tbl_iterator_ = hash_tbl_->End();
  out_batch->CommitRows(rows_returned);
  return rows_returned;
}

void HashJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  // insert build row into our hash table
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    hash_tbl_->Insert(build_batch->GetRow(i));
  }
}
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
