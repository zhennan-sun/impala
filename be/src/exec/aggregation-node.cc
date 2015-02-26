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

#include "exec/aggregation-node.h"

#include <math.h>
#include <sstream>
#include <boost/functional/hash.hpp>
<<<<<<< HEAD

#include <x86intrin.h>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-expr.h"
#include "exprs/expr.h"
=======
#include <thrift/protocol/TDebugProtocol.h>

#include <x86intrin.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/old-hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
<<<<<<< HEAD
=======
#include "udf/udf-internal.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace llvm;

<<<<<<< HEAD
// This object appends n-int32s to the end of a normal tuple object to maintain the
// lengths of the string buffers in the tuple.
namespace impala {

const char* AggregationNode::LLVM_CLASS_NAME = "class.impala::AggregationNode";
const int AggregationNode::NUM_PC_BITMAPS = 64;
const int AggregationNode::PC_BITMAP_LENGTH = 32;
const float AggregationNode::PC_THETA = 0.77351;

class AggregationTuple {
 public:
  static AggregationTuple* Create(int tuple_size, int num_string_slots, MemPool* pool) {
    int size = tuple_size + sizeof(int32_t) * num_string_slots;
    AggregationTuple* result = reinterpret_cast<AggregationTuple*>(pool->Allocate(size));
    result->Init(size);
    return result;
  }

  void Init(int size) {
    bzero(this, size);
  }
    
  Tuple* tuple() { return reinterpret_cast<Tuple*>(this); }

  int32_t* BufferLengths(int tuple_size) {
    char* data = reinterpret_cast<char*>(this) + tuple_size;
    return reinterpret_cast<int*>(data);
  }

  // For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  void* data_;
};

const char* AggregationTuple::LLVM_CLASS_NAME = "class.impala::AggregationTuple";

// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::Init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    agg_tuple_id_(tnode.agg_node.agg_tuple_id),
    agg_tuple_desc_(NULL),
    singleton_output_tuple_(NULL),
    num_string_slots_(0),
    tuple_pool_(new MemPool()),
    codegen_process_row_batch_fn_(NULL),
    process_row_batch_fn_(NULL),
    needs_finalize_(tnode.agg_node.need_finalize) {
  // ignore return status for now
  Expr::CreateExprTrees(pool, tnode.agg_node.grouping_exprs, &probe_exprs_);
  Expr::CreateExprTrees(pool, tnode.agg_node.aggregate_exprs, &aggregate_exprs_);
}

Status AggregationNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  build_timer_ =
      ADD_COUNTER(runtime_profile(), "BuildTime", TCounterType::CPU_TICKS);
  get_results_timer_ =
      ADD_COUNTER(runtime_profile(), "GetResultsTime", TCounterType::CPU_TICKS);
  hash_table_buckets_counter_ = 
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TCounterType::UNIT);

  SCOPED_TIMER(runtime_profile_->total_time_counter());
  
  agg_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(agg_tuple_id_);
  RETURN_IF_ERROR(Expr::Prepare(probe_exprs_, state, child(0)->row_desc()));
  RETURN_IF_ERROR(Expr::Prepare(aggregate_exprs_, state, child(0)->row_desc()));

  // Construct build exprs from agg_tuple_desc_
  for (int i = 0; i < probe_exprs_.size(); ++i) {
    SlotDescriptor* desc = agg_tuple_desc_->slots()[i];
    Expr* expr = new SlotRef(desc);      
    state->obj_pool()->Add(expr);
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(Expr::Prepare(build_exprs_, state, row_desc()));

  // TODO: how many buckets?
  hash_tbl_.reset(new HashTable(build_exprs_, probe_exprs_, 1, true));
  
  // Determine the number of string slots in the output
  for (vector<Expr*>::const_iterator expr = aggregate_exprs_.begin();
       expr != aggregate_exprs_.end(); ++expr) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);
    if (agg_expr->type() == TYPE_STRING) ++num_string_slots_;
  }
  
  if (probe_exprs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ = ConstructAggTuple();
  }

  LlvmCodeGen* codegen = state->llvm_codegen();
  if (codegen != NULL) {
    Function* update_tuple_fn = CodegenUpdateAggTuple(codegen);
    if (update_tuple_fn != NULL) {
      codegen_process_row_batch_fn_ = CodegenProcessRowBatch(codegen, update_tuple_fn);
=======
namespace impala {

const char* AggregationNode::LLVM_CLASS_NAME = "class.impala::AggregationNode";

// TODO: pass in maximum size; enforce by setting limit in mempool
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_id_(tnode.agg_node.intermediate_tuple_id),
    intermediate_tuple_desc_(NULL),
    output_tuple_id_(tnode.agg_node.output_tuple_id),
    output_tuple_desc_(NULL),
    singleton_intermediate_tuple_(NULL),
    codegen_process_row_batch_fn_(NULL),
    process_row_batch_fn_(NULL),
    needs_finalize_(tnode.agg_node.need_finalize),
    build_timer_(NULL),
    get_results_timer_(NULL),
    hash_table_buckets_counter_(NULL) {
}

Status AggregationNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.agg_node.grouping_exprs, &probe_expr_ctxs_));
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
        pool_, tnode.agg_node.aggregate_functions[i], &evaluator));
    aggregate_evaluators_.push_back(evaluator);
  }
  return Status::OK;
}

Status AggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  tuple_pool_.reset(new MemPool(mem_tracker()));
  agg_fn_pool_.reset(new MemPool(expr_mem_tracker()));
  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  hash_table_buckets_counter_ =
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TCounterType::UNIT);
  hash_table_load_factor_counter_ =
      ADD_COUNTER(runtime_profile(), "LoadFactor", TCounterType::DOUBLE_VALUE);

  intermediate_tuple_desc_ =
      state->desc_tbl().GetTupleDescriptor(intermediate_tuple_id_);
  output_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(output_tuple_id_);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(),
      output_tuple_desc_->slots().size());
  RETURN_IF_ERROR(
      Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc(), expr_mem_tracker()));

  // Construct build exprs from intermediate_agg_tuple_desc_
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
    SlotDescriptor* desc = intermediate_tuple_desc_->slots()[i];
    DCHECK(desc->type().type == TYPE_NULL ||
           desc->type() == probe_expr_ctxs_[i]->root()->type());
    // TODO: Generate the build exprs in the FE such that the existing logic
    // for handling NULL_TYPE works.
    // Hack to avoid TYPE_NULL SlotRefs.
    Expr* expr = desc->type().type != TYPE_NULL ?
        new SlotRef(desc) : new SlotRef(desc, TYPE_BOOLEAN);
    state->obj_pool()->Add(expr);
    build_expr_ctxs_.push_back(new ExprContext(expr));
    state->obj_pool()->Add(build_expr_ctxs_.back());
  }
  // Construct a new row desc for preparing the build exprs because neither the child's
  // nor this node's output row desc may contain the intermediate tuple, e.g.,
  // in a single-node plan with an intermediate tuple different from the output tuple.
  RowDescriptor build_row_desc(intermediate_tuple_desc_, false);
  RETURN_IF_ERROR(
      Expr::Prepare(build_expr_ctxs_, state, build_row_desc, expr_mem_tracker()));

  agg_fn_ctxs_.resize(aggregate_evaluators_.size());
  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1)
          << "#eval= " << aggregate_evaluators_.size()
          << " #probe=" << probe_expr_ctxs_.size();
      ++j;
    }
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_slot_desc, output_slot_desc, agg_fn_pool_.get(), &agg_fn_ctxs_[i]));
    state->obj_pool()->Add(agg_fn_ctxs_[i]);
  }

  // TODO: how many buckets?
  hash_tbl_.reset(new OldHashTable(state, build_expr_ctxs_, probe_expr_ctxs_, 1,
                                   true, true, id(), mem_tracker(), true));

  if (probe_expr_ctxs_.empty()) {
    // create single intermediate tuple now; we need to output something
    // even if our input is empty
    singleton_intermediate_tuple_ = ConstructIntermediateTuple();
    hash_tbl_->Insert(singleton_intermediate_tuple_);
    output_iterator_ = hash_tbl_->Begin();
  }

  if (state->codegen_enabled()) {
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));
    Function* update_tuple_fn = CodegenUpdateTuple(state);
    if (update_tuple_fn != NULL) {
      codegen_process_row_batch_fn_ =
          CodegenProcessRowBatch(state, update_tuple_fn);
      if (codegen_process_row_batch_fn_ != NULL) {
        // Update to using codegen'd process row batch.
        codegen->AddFunctionToJit(codegen_process_row_batch_fn_,
            reinterpret_cast<void**>(&process_row_batch_fn_));
        AddRuntimeExecOption("Codegen Enabled");
      }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
  return Status::OK;
}

Status AggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
<<<<<<< HEAD

  // Update to using codegen'd process row batch.
  if (codegen_process_row_batch_fn_ != NULL) {
    void* jitted_process_row_batch = 
        state->llvm_codegen()->JitFunction(codegen_process_row_batch_fn_);
    process_row_batch_fn_ = 
        reinterpret_cast<ProcessRowBatchFn>(jitted_process_row_batch);
    LOG(INFO) << "AggregationNode(node_id=" << id() 
              << ") using llvm codegend functions.";
=======
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));

  DCHECK_EQ(aggregate_evaluators_.size(), agg_fn_ctxs_.size());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Open(state, agg_fn_ctxs_[i]));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  RETURN_IF_ERROR(children_[0]->Open(state));

<<<<<<< HEAD
  RowBatch batch(children_[0]->row_desc(), state->batch_size());
  int64_t num_input_rows = 0;
  int64_t num_agg_rows = 0;
  while (true) {
    bool eos;
    RETURN_IF_CANCELLED(state);
=======
  RowBatch batch(children_[0]->row_desc(), state->batch_size(), mem_tracker());
  int64_t num_input_rows = 0;
  while (true) {
    bool eos;
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));
    SCOPED_TIMER(build_timer_);

    if (VLOG_ROW_IS_ON) {
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        VLOG_ROW << "input row: " << PrintRow(row, children_[0]->row_desc());
      }
    }
<<<<<<< HEAD
    int64_t agg_rows_before = hash_tbl_->size();
    if (process_row_batch_fn_ != NULL) {
      process_row_batch_fn_(this, &batch);
    } else if (singleton_output_tuple_ != NULL) {
=======
    if (process_row_batch_fn_ != NULL) {
      process_row_batch_fn_(this, &batch);
    } else if (probe_expr_ctxs_.empty()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      ProcessRowBatchNoGrouping(&batch);
    } else {
      ProcessRowBatchWithGrouping(&batch);
    }
    COUNTER_SET(hash_table_buckets_counter_, hash_tbl_->num_buckets());
<<<<<<< HEAD
    COUNTER_SET(memory_used_counter(), 
        tuple_pool_->peak_allocated_bytes() + hash_tbl_->byte_size());
    num_agg_rows += (hash_tbl_->size() - agg_rows_before);
    num_input_rows += batch.num_rows();

    batch.Reset();
    if (eos) break;
  }
  
  if (singleton_output_tuple_ != NULL) {
    hash_tbl_->Insert(reinterpret_cast<TupleRow*>(&singleton_output_tuple_));
    ++num_agg_rows;
  }
  VLOG_FILE << "aggregated " << num_input_rows << " input rows into "
            << num_agg_rows << " output rows";
  output_iterator_ = hash_tbl_->Begin();
=======
    COUNTER_SET(hash_table_load_factor_counter_, hash_tbl_->load_factor());
    num_input_rows += batch.num_rows();
    // We must set output_iterator_ here, rather than outside the loop, because
    // output_iterator_ must be set if the function returns within the loop
    output_iterator_ = hash_tbl_->Begin();

    batch.Reset();
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (eos) break;
  }

  // We have consumed all of the input from the child and transfered ownership of the
  // resources we need, so the child can be closed safely to release its resources.
  child(0)->Close(state);
  VLOG_FILE << "aggregated " << num_input_rows << " input rows into "
            << hash_tbl_->size() << " output rows";
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
<<<<<<< HEAD
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
=======
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  SCOPED_TIMER(get_results_timer_);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }
<<<<<<< HEAD
  Expr** conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

  while (output_iterator_.HasNext() && !row_batch->IsFull()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* agg_tuple = output_iterator_.GetRow()->GetTuple(0);
    if (needs_finalize_) {
      FinalizeAggTuple(reinterpret_cast<AggregationTuple*>(agg_tuple));
    }
    row->SetTuple(0, agg_tuple);
    if (ExecNode::EvalConjuncts(conjuncts, num_conjuncts, row)) {
=======
  *eos = false;
  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();

  int count = 0;
  const int N = state->batch_size();
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
    // This loop can go on for a long time if the conjuncts are very selective. Do query
    // maintenance every N iterations.
    if (count++ % N == 0) {
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    Tuple* output_tuple =
        FinalizeTuple(intermediate_tuple, row_batch->tuple_data_pool());
    output_iterator_.Next<false>();
    row->SetTuple(0, output_tuple);
    if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      VLOG_ROW << "output row: " << PrintRow(row, row_desc());
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break;
    }
<<<<<<< HEAD
    output_iterator_.Next<false>();
  }
  *eos = !output_iterator_.HasNext() || ReachedLimit();
=======
  }
  *eos = output_iterator_.AtEnd() || ReachedLimit();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

<<<<<<< HEAD
Status AggregationNode::Close(RuntimeState* state) {
  COUNTER_SET(memory_used_counter(), 
      tuple_pool_->peak_allocated_bytes() + hash_tbl_->byte_size());
  COUNTER_SET(hash_table_buckets_counter_, hash_tbl_->num_buckets());
  return ExecNode::Close(state);
}

AggregationTuple* AggregationNode::ConstructAggTuple() {
  AggregationTuple* agg_out_tuple = 
      AggregationTuple::Create(agg_tuple_desc_->byte_size(), 
          num_string_slots_, tuple_pool_.get());
  Tuple* agg_tuple = agg_out_tuple->tuple();

  vector<SlotDescriptor*>::const_iterator slot_desc = agg_tuple_desc_->slots().begin();
  // copy grouping values
  for (int i = 0; i < probe_exprs_.size(); ++i, ++slot_desc) {
    if (hash_tbl_->last_expr_value_null(i)) {
      agg_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = agg_tuple->GetSlot((*slot_desc)->tuple_offset());
=======
void AggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs
  while (!output_iterator_.AtEnd()) {
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    if (FinalizeTuple(intermediate_tuple, tuple_pool_.get()) != intermediate_tuple) {
      // Avoid consuming excessive memory.
      tuple_pool_->FreeAll();
    }
    output_iterator_.Next<false>();
  }

  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();

  DCHECK(agg_fn_ctxs_.empty() || aggregate_evaluators_.size() == agg_fn_ctxs_.size());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    aggregate_evaluators_[i]->Close(state);
    if (!agg_fn_ctxs_.empty()) agg_fn_ctxs_[i]->impl()->Close();
  }
  if (agg_fn_pool_.get() != NULL) agg_fn_pool_->FreeAll();

  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

Tuple* AggregationNode::ConstructIntermediateTuple() {
  Tuple* intermediate_tuple = Tuple::Create(
      intermediate_tuple_desc_->byte_size(), tuple_pool_.get());
  vector<SlotDescriptor*>::const_iterator slot_desc =
      intermediate_tuple_desc_->slots().begin();

  // copy grouping values
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i, ++slot_desc) {
    if (hash_tbl_->last_expr_value_null(i)) {
      intermediate_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = intermediate_tuple->GetSlot((*slot_desc)->tuple_offset());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      RawValue::Write(src, dst, (*slot_desc)->type(), tuple_pool_.get());
    }
  }

<<<<<<< HEAD
  int string_slot_idx = -1;

  ExprValue default_value;

  for (int i = 0; i < aggregate_exprs_.size(); ++i, ++slot_desc) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    // To minimize branching on the UpdateAggTuple path, initialize the result value
    // so that UpdateAggTuple doesn't have to check if the aggregation dst slot is null.
    //  - sum/count: 0
    //  - min: max_value
    //  - max: min_value
    if (agg_expr->type() != TYPE_STRING && agg_expr->type() != TYPE_TIMESTAMP) {
      void* default_value_ptr = NULL;
      switch (agg_expr->agg_op()) {
        case TAggregationOp::MIN:
          default_value_ptr = default_value.SetToMax(agg_expr->type());
          break;
        case TAggregationOp::MAX:
          default_value_ptr = default_value.SetToMin(agg_expr->type());
          break;
        default:
          default_value_ptr = default_value.SetToZero(agg_expr->type());
          break;
      }
      RawValue::Write(default_value_ptr, agg_tuple, *slot_desc, NULL);
    }
    
    // All aggregate values except for COUNT start out with NULL
    // (so that SUM(<col>) stays NULL if <col> only contains NULL values).
    if ((*slot_desc)->is_nullable()) {
      DCHECK_NE(agg_expr->agg_op(), TAggregationOp::COUNT);
      agg_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } 

    // Keep track of how many string slots we have seen, in order to know the index of the
    // current slot in the array of string buffer lengths
    if (agg_expr->type() == TYPE_STRING) {
      ++string_slot_idx;
    }

    // Construct Distinct Estimate bitmap
    void* slot = agg_tuple->GetSlot((*slot_desc)->tuple_offset());
    if (agg_expr->agg_op() == TAggregationOp::DISTINCT_PC ||
        agg_expr->agg_op() == TAggregationOp::DISTINCT_PCSA ||
        agg_expr->agg_op() == TAggregationOp::MERGE_PC ||
        agg_expr->agg_op() == TAggregationOp::MERGE_PCSA) {
      ConstructDistinctEstimateSlot(agg_out_tuple,
          (*slot_desc)->null_indicator_offset(), string_slot_idx, slot);
    }
  }

  return agg_out_tuple;
}

char* AggregationNode::AllocateStringBuffer(int new_size, int* allocated_size) {
  new_size = ::max(new_size, FreeList::MinSize());
  char* buffer = reinterpret_cast<char*>(
      string_buffer_free_list_.Allocate(new_size, allocated_size));
  if (buffer == NULL)  {
    buffer = reinterpret_cast<char*>(tuple_pool_->Allocate(new_size));
    *allocated_size = new_size;
  }
  return buffer;
}

inline void AggregationNode::UpdateStringSlot(AggregationTuple* tuple,
    int string_slot_idx, StringValue* dst, const StringValue* src) {
  int32_t* string_buffer_lengths = tuple->BufferLengths(agg_tuple_desc_->byte_size());
  int curr_size = string_buffer_lengths[string_slot_idx];
  if (curr_size < src->len) {
    string_buffer_free_list_.Add(reinterpret_cast<uint8_t*>(dst->ptr), curr_size);
    dst->ptr = AllocateStringBuffer(src->len, &(string_buffer_lengths[string_slot_idx]));
  }
  strncpy(dst->ptr, src->ptr, src->len);
  dst->len = src->len;
}

inline void AggregationNode::UpdateMinStringSlot(AggregationTuple* agg_tuple, 
    const NullIndicatorOffset& null_indicator_offset, int string_slot_idx,
    void* slot, void* value) {
  DCHECK(value != NULL);
  Tuple* tuple = agg_tuple->tuple();
  StringValue* dst_value = static_cast<StringValue*>(slot);
  StringValue* src_value = static_cast<StringValue*>(value);

  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
  } else if (src_value->Compare(*dst_value) >= 0) {
    return;
  }
  UpdateStringSlot(agg_tuple, string_slot_idx, dst_value, src_value);
}

inline void AggregationNode::UpdateMaxStringSlot(AggregationTuple* agg_tuple, 
    const NullIndicatorOffset& null_indicator_offset, int string_slot_idx,
    void* slot, void* value) {
  DCHECK(value != NULL);
  Tuple* tuple = agg_tuple->tuple();
  StringValue* dst_value = static_cast<StringValue*>(slot);
  StringValue* src_value = static_cast<StringValue*>(value);

  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
  } else if (src_value->Compare(*dst_value) <= 0) {
    return;
  }
  UpdateStringSlot(agg_tuple, string_slot_idx, dst_value, src_value);
}

template <typename T>
void UpdateMinSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = min(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateMaxSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = max(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateSumSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot += *static_cast<T*>(value);
  }
}

void AggregationNode::UpdateAggTuple(AggregationTuple* agg_out_tuple, TupleRow* row) {
  DCHECK(agg_out_tuple != NULL);
  Tuple* tuple = agg_out_tuple->tuple();
  int string_slot_idx = -1;
  vector<SlotDescriptor*>::const_iterator slot_desc =
      agg_tuple_desc_->slots().begin() + probe_exprs_.size();
  for (vector<Expr*>::const_iterator expr = aggregate_exprs_.begin();
      expr != aggregate_exprs_.end(); ++expr, ++slot_desc) {
    void* slot = tuple->GetSlot((*slot_desc)->tuple_offset());
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

    // Keep track of how many string slots we have seen, in order to know the index of the
    // current slot in the array of string buffer lengths
    if (agg_expr->type() == TYPE_STRING) ++string_slot_idx;

    // deal with COUNT(*) separately (no need to check the actual child expr value)
    if (agg_expr->is_star()) {
      DCHECK_EQ(agg_expr->agg_op(), TAggregationOp::COUNT);
      // we're only aggregating into bigint slots
      DCHECK_EQ((*slot_desc)->type(), TYPE_BIGINT);
      ++*reinterpret_cast<int64_t*>(slot);
      continue;
    }

    // determine value of aggregate's child expr
    void* value = agg_expr->GetChild(0)->GetValue(row);
    if (value == NULL) {
      // NULLs don't get aggregated
      continue;
    }

    switch (agg_expr->agg_op()) {
      case TAggregationOp::COUNT:
        ++*reinterpret_cast<int64_t*>(slot);
        break;

      case TAggregationOp::MIN:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMinSlot<bool>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_TINYINT:
            UpdateMinSlot<int8_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_SMALLINT:
            UpdateMinSlot<int16_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_INT:
            UpdateMinSlot<int32_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_BIGINT:
            UpdateMinSlot<int64_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_FLOAT:
            UpdateMinSlot<float>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_DOUBLE:
            UpdateMinSlot<double>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_TIMESTAMP:
            UpdateMinSlot<TimestampValue>(tuple, (*slot_desc)->null_indicator_offset(),
                slot, value);
            break;
          case TYPE_STRING:
            UpdateMinStringSlot(agg_out_tuple, (*slot_desc)->null_indicator_offset(),
                string_slot_idx, slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TAggregationOp::MAX:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMaxSlot<bool>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_TINYINT:
            UpdateMaxSlot<int8_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_SMALLINT:
            UpdateMaxSlot<int16_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_INT:
            UpdateMaxSlot<int32_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_BIGINT:
            UpdateMaxSlot<int64_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_FLOAT:
            UpdateMaxSlot<float>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_DOUBLE:
            UpdateMaxSlot<double>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_TIMESTAMP:
            UpdateMaxSlot<TimestampValue>(tuple, (*slot_desc)->null_indicator_offset(),
                slot, value);
            break;
          case TYPE_STRING:
            UpdateMaxStringSlot(agg_out_tuple, (*slot_desc)->null_indicator_offset(),
                string_slot_idx, slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TAggregationOp::SUM:
        switch (agg_expr->type()) {
          case TYPE_BIGINT:
            UpdateSumSlot<int64_t>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          case TYPE_DOUBLE:
            UpdateSumSlot<double>(tuple, (*slot_desc)->null_indicator_offset(), slot,
                value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TAggregationOp::DISTINCT_PC:
        UpdateDistinctEstimateSlot(slot, value, agg_expr->GetChild(0)->type());
        break;

      case TAggregationOp::DISTINCT_PCSA:
        UpdateDistinctEstimatePCSASlot(slot, value, agg_expr->GetChild(0)->type());
        break;

      case TAggregationOp::MERGE_PCSA:
      case TAggregationOp::MERGE_PC:
        DCHECK_EQ(agg_expr->GetChild(0)->type(), TYPE_STRING);
        UpdateMergeEstimateSlot(agg_out_tuple, string_slot_idx, slot, value);
        break;

      default:
        DCHECK(false) << "bad aggregate operator: " << agg_expr->agg_op();
    }
  }
}

void AggregationNode::FinalizeAggTuple(AggregationTuple* agg_out_tuple) {
  DCHECK(agg_out_tuple != NULL);
  Tuple* tuple = agg_out_tuple->tuple();
  int string_slot_idx = -1;
  vector<SlotDescriptor*>::const_iterator slot_desc =
      agg_tuple_desc_->slots().begin() + probe_exprs_.size();
  for (vector<Expr*>::const_iterator expr = aggregate_exprs_.begin();
      expr != aggregate_exprs_.end(); ++expr, ++slot_desc) {
    void* slot = tuple->GetSlot((*slot_desc)->tuple_offset());
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

    // Keep track of how many string slots we have seen, in order to know the index of the
    // current slot in the array of string buffer lengths
    if (agg_expr->type() == TYPE_STRING) ++string_slot_idx;

    switch (agg_expr->agg_op()) {
      // Only DISTINCT/MERGE_PC(SA) needs to do finalize
      case TAggregationOp::DISTINCT_PC:
      case TAggregationOp::MERGE_PC:
      case TAggregationOp::DISTINCT_PCSA:
      case TAggregationOp::MERGE_PCSA:
        // Convert the bit vector into a number
        FinalizeEstimateSlot(string_slot_idx, slot, agg_expr->agg_op());
        break;
        // For all other aggregate, do nothing.
      default:
        break;
    }
  }
=======
  // Initialize aggregate output.
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++slot_desc) {
    while (!(*slot_desc)->is_materialized()) ++slot_desc;
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    evaluator->Init(agg_fn_ctxs_[i], intermediate_tuple);
    // Codegen specific path.
    // To minimize branching on the UpdateTuple path, initialize the result value
    // so that UpdateTuple doesn't have to check if the aggregation
    // dst slot is null.
    //  - sum/count: 0
    //  - min: max_value
    //  - max: min_value
    // TODO: remove when we don't use the irbuilder for codegen here.
    // This optimization no longer applies with AnyVal
    if ((*slot_desc)->type().type != TYPE_STRING &&
        (*slot_desc)->type().type != TYPE_VARCHAR &&
        (*slot_desc)->type().type != TYPE_TIMESTAMP &&
        (*slot_desc)->type().type != TYPE_CHAR &&
        (*slot_desc)->type().type != TYPE_DECIMAL) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      switch (evaluator->agg_op()) {
        case AggFnEvaluator::MIN:
          default_value_ptr = default_value.SetToMax((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        case AggFnEvaluator::MAX:
          default_value_ptr = default_value.SetToMin((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        default:
          break;
      }
    }
  }
  return intermediate_tuple;
}

void AggregationNode::UpdateTuple(Tuple* tuple, TupleRow* row) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  AggFnEvaluator::Add(aggregate_evaluators_, agg_fn_ctxs_, row, tuple);
}

Tuple* AggregationNode::FinalizeTuple(Tuple* tuple, MemPool* pool) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  Tuple* dst = tuple;
  if (needs_finalize_ && intermediate_tuple_id_ != output_tuple_id_) {
    dst = Tuple::Create(output_tuple_desc_->byte_size(), pool);
  }
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    FunctionContext* agg_fn_ctx = agg_fn_ctxs_[i];
    if (needs_finalize_) {
      evaluator->Finalize(agg_fn_ctx, tuple, dst);
    } else {
      evaluator->Serialize(agg_fn_ctx, tuple);
    }
  }
  // Copy grouping values from tuple to dst.
  // TODO: Codegen this.
  if (dst != tuple) {
    int num_grouping_slots = probe_expr_ctxs_.size();
    for (int i = 0; i < num_grouping_slots; ++i) {
      SlotDescriptor* src_slot_desc = intermediate_tuple_desc_->slots()[i];
      SlotDescriptor* dst_slot_desc = output_tuple_desc_->slots()[i];
      bool src_slot_null = tuple->IsNull(src_slot_desc->null_indicator_offset());
      void* src_slot = NULL;
      if (!src_slot_null) src_slot = tuple->GetSlot(src_slot_desc->tuple_offset());
      RawValue::Write(src_slot, dst, dst_slot_desc, NULL);
    }
  }
  return dst;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
<<<<<<< HEAD
  *out << "AggregationNode(tuple_id=" << agg_tuple_id_
       << " probe_exprs=" << Expr::DebugString(probe_exprs_)
       << " agg_exprs=" << Expr::DebugString(aggregate_exprs_);
=======
  *out << "AggregationNode("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_
       << " needs_finalize=" << needs_finalize_
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

<<<<<<< HEAD
// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(AggTuple* agg_tuple, char** row)
// The IR for sum(double_col) is:
//  define void @UpdateSlot({ i8, double }* %agg_tuple, i8** %row) {
//  entry:
//    %src_null_ptr = alloca i1
//    %src_value = call double @SlotRef(i8** %row, i8* null, i1* %src_null_ptr)
//    %src_is_null = load i1* %src_null_ptr
//    br i1 %src_is_null, label %ret, label %src_not_null
//  
//  src_not_null:                                     ; preds = %entry
//    %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//    call void @SetNotNull({ i8, double }* %agg_tuple)
//    %dst_val = load double* %dst_slot_ptr
//    %0 = fadd double %dst_val, %src_value
//    store double %0, double* %dst_slot_ptr
//    br label %ret
//  
//  ret:                                    ; preds = %src_not_null, %entry
//    ret void
//  }
llvm::Function* AggregationNode::CodegenUpdateSlot(LlvmCodeGen* codegen, int slot_idx) {
  AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[slot_idx]);
  SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[probe_exprs_.size() + slot_idx];
  int field_idx = slot_desc->field_idx();

  LLVMContext& context = codegen->context();

  StructType* tuple_struct = agg_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  PointerType* ptr_type = codegen->ptr_type();
  
  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", PointerType::get(ptr_type, 0)));

  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  LlvmCodeGen::NamedVariable null_var("src_null_ptr", codegen->boolean_type());
  Value* src_is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

  // Call expr function to get src slot value
  Function* agg_expr_fn = agg_expr->codegen_fn();
  int scratch_buffer_size = agg_expr->scratch_buffer_size();
  DCHECK_EQ(scratch_buffer_size, 0);
  DCHECK(agg_expr_fn != NULL);
  if (agg_expr_fn == NULL) return NULL;
  
  BasicBlock* src_not_null_block, *ret_block;
  codegen->CreateIfElseBlocks(fn, "src_not_null", "ret", &src_not_null_block, &ret_block);

  Value* expr_args[] = { args[1], ConstantPointerNull::get(ptr_type), src_is_null_ptr };
  Value* src_value = agg_expr->CodegenGetValue(codegen, builder.GetInsertBlock(),
      expr_args, ret_block, src_not_null_block);
  
  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr = builder.CreateStructGEP(args[0], field_idx, "dst_slot_ptr");
  Value* result = NULL;
    
  if (slot_desc->is_nullable()) {
    // Dst is NULL, just update dst slot to src slot and clear null bit
    Function* clear_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_struct, false);
    builder.CreateCall(clear_null_fn, args[0]);
  }
    
  // Update the slot
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (agg_expr->agg_op()) {
    case TAggregationOp::COUNT:
      result = builder.CreateAdd(dst_value, 
          codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      break;
    case TAggregationOp::MIN: {
      Function* min_fn = codegen->CodegenMinMax(agg_expr->type(), true);
      Value* min_args[] = { dst_value, src_value };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case TAggregationOp::MAX: {
      Function* max_fn = codegen->CodegenMinMax(agg_expr->type(), false);
      Value* max_args[] = { dst_value, src_value };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case TAggregationOp::SUM:
      if (agg_expr->type() == TYPE_FLOAT || agg_expr->type() == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src_value);
      } else {
        result = builder.CreateAdd(dst_value, src_value);
      }
      break;
    default:
      DCHECK(false) << "bad aggregate operator: " << agg_expr->agg_op();
  }
    
=======
IRFunction::Type GetHllUpdateFunction2(const ColumnType& type) {
  switch (type.type) {
    case TYPE_BOOLEAN: return IRFunction::HLL_UPDATE_BOOLEAN;
    case TYPE_TINYINT: return IRFunction::HLL_UPDATE_TINYINT;
    case TYPE_SMALLINT: return IRFunction::HLL_UPDATE_SMALLINT;
    case TYPE_INT: return IRFunction::HLL_UPDATE_INT;
    case TYPE_BIGINT: return IRFunction::HLL_UPDATE_BIGINT;
    case TYPE_FLOAT: return IRFunction::HLL_UPDATE_FLOAT;
    case TYPE_DOUBLE: return IRFunction::HLL_UPDATE_DOUBLE;
    case TYPE_STRING: return IRFunction::HLL_UPDATE_STRING;
    case TYPE_DECIMAL: return IRFunction::HLL_UPDATE_DECIMAL;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return IRFunction::FN_END;
  }
}

// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(FunctionContext* fn_ctx, AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         { i8, double }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %src = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//     (i64 128241264 to %"class.impala::ExprContext"*), %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, double }* %agg_tuple)
//   %dst_val = load double* %dst_slot_ptr
//   %val = extractvalue { i8, double } %src, 1
//   %1 = fadd double %dst_val, %val
//   store double %1, double* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
//
// The IR for ndv(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         { i8, %"struct.impala::StringValue" }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i8* }
//   %src_lowered_ptr = alloca { i8, double }
//   %src = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//     (i64 120530832 to %"class.impala::ExprContext"*), %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds
//     { i8, %"struct.impala::StringValue" }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, %"struct.impala::StringValue" }* %agg_tuple)
//   %dst_val = load %"struct.impala::StringValue"* %dst_slot_ptr
//   store { i8, double } %src, { i8, double }* %src_lowered_ptr
//   %src_unlowered_ptr = bitcast { i8, double }* %src_lowered_ptr
//                        to %"struct.impala_udf::DoubleVal"*
//   %ptr = extractvalue %"struct.impala::StringValue" %dst_val, 0
//   %dst_stringval = insertvalue { i64, i8* } zeroinitializer, i8* %ptr, 1
//   %len = extractvalue %"struct.impala::StringValue" %dst_val, 1
//   %1 = extractvalue { i64, i8* } %dst_stringval, 0
//   %2 = zext i32 %len to i64
//   %3 = shl i64 %2, 32
//   %4 = and i64 %1, 4294967295
//   %5 = or i64 %4, %3
//   %dst_stringval1 = insertvalue { i64, i8* } %dst_stringval, i64 %5, 0
//   store { i64, i8* } %dst_stringval1, { i64, i8* }* %dst_lowered_ptr
//   %dst_unlowered_ptr = bitcast { i64, i8* }* %dst_lowered_ptr
//                        to %"struct.impala_udf::StringVal"*
//   call void @HllUpdate(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                        %"struct.impala_udf::DoubleVal"* %src_unlowered_ptr,
//                        %"struct.impala_udf::StringVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i8* }* %dst_lowered_ptr
//   %6 = extractvalue { i64, i8* } %anyval_result, 1
//   %7 = insertvalue %"struct.impala::StringValue" zeroinitializer, i8* %6, 0
//   %8 = extractvalue { i64, i8* } %anyval_result, 0
//   %9 = ashr i64 %8, 32
//   %10 = trunc i64 %9 to i32
//   %11 = insertvalue %"struct.impala::StringValue" %7, i32 %10, 1
//   store %"struct.impala::StringValue" %11, %"struct.impala::StringValue"* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
llvm::Function* AggregationNode::CodegenUpdateSlot(
    RuntimeState* state, AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
  DCHECK(slot_desc->is_materialized());
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;

  DCHECK_EQ(evaluator->input_expr_ctxs().size(), 1);
  ExprContext* input_expr_ctx = evaluator->input_expr_ctxs()[0];
  Expr* input_expr = input_expr_ctx->root();
  // TODO: implement timestamp
  if (input_expr->type().type == TYPE_TIMESTAMP) return NULL;
  Function* agg_expr_fn;
  Status status = input_expr->GetCodegendComputeFn(state, &agg_expr_fn);
  if (!status.ok()) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): " << status.GetErrorMsg();
    return NULL;
  }
  DCHECK(agg_expr_fn != NULL);

  PointerType* fn_ctx_type =
      codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  StructType* tuple_struct = intermediate_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr_type = PointerType::get(tuple_struct, 0);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);

  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fn_ctx", fn_ctx_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
  Value* fn_ctx_arg = args[0];
  Value* agg_tuple_arg = args[1];
  Value* row_arg = args[2];

  BasicBlock* src_not_null_block =
      BasicBlock::Create(codegen->context(), "src_not_null", fn);
  BasicBlock* ret_block = BasicBlock::Create(codegen->context(), "ret", fn);

  // Call expr function to get src slot value
  Value* ctx_arg = codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME), input_expr_ctx);
  Value* agg_expr_fn_args[] = { ctx_arg, row_arg };
  CodegenAnyVal src = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, input_expr->type(), agg_expr_fn, agg_expr_fn_args, "src");

  Value* src_is_null = src.GetIsNull();
  builder.CreateCondBr(src_is_null, ret_block, src_not_null_block);

  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr =
      builder.CreateStructGEP(agg_tuple_arg, slot_desc->field_idx(), "dst_slot_ptr");
  Value* result = NULL;

  if (slot_desc->is_nullable()) {
    // Dst is NULL, just update dst slot to src slot and clear null bit
    Function* clear_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_struct, false);
    builder.CreateCall(clear_null_fn, agg_tuple_arg);
  }

  // Update the slot
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (evaluator->agg_op()) {
    case AggFnEvaluator::COUNT:
      if (evaluator->is_merge()) {
        result = builder.CreateAdd(dst_value, src.GetVal(), "count_sum");
      } else {
        result = builder.CreateAdd(dst_value,
            codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      }
      break;
    case AggFnEvaluator::MIN: {
      Function* min_fn = codegen->CodegenMinMax(slot_desc->type(), true);
      Value* min_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case AggFnEvaluator::MAX: {
      Function* max_fn = codegen->CodegenMinMax(slot_desc->type(), false);
      Value* max_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case AggFnEvaluator::SUM:
      if (slot_desc->type().type == TYPE_FLOAT || slot_desc->type().type == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src.GetVal());
      } else {
        result = builder.CreateAdd(dst_value, src.GetVal());
      }
      break;
    case AggFnEvaluator::NDV: {
      DCHECK_EQ(slot_desc->type().type, TYPE_STRING);
      IRFunction::Type ir_function_type = evaluator->is_merge() ? IRFunction::HLL_MERGE
                                          : GetHllUpdateFunction2(input_expr->type());
      Function* hll_fn = codegen->GetFunction(ir_function_type);

      // Create pointer to src_anyval to pass to HllUpdate() function. We must use the
      // unlowered type.
      Value* src_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("src_lowered_ptr", src.value()->getType()));
      builder.CreateStore(src.value(), src_lowered_ptr);
      Type* unlowered_ptr_type =
          CodegenAnyVal::GetUnloweredType(codegen, input_expr->type())->getPointerTo();
      Value* src_unlowered_ptr =
          builder.CreateBitCast(src_lowered_ptr, unlowered_ptr_type, "src_unlowered_ptr");

      // Create StringVal* intermediate argument from dst_value
      CodegenAnyVal dst_stringval = CodegenAnyVal::GetNonNullVal(
          codegen, &builder, TYPE_STRING, "dst_stringval");
      dst_stringval.SetFromRawValue(dst_value);
      // Create pointer to dst_stringval to pass to HllUpdate() function. We must use
      // the unlowered type.
      Value* dst_lowered_ptr = codegen->CreateEntryBlockAlloca(
          fn, LlvmCodeGen::NamedVariable("dst_lowered_ptr",
                                         dst_stringval.value()->getType()));
      builder.CreateStore(dst_stringval.value(), dst_lowered_ptr);
      unlowered_ptr_type =
          codegen->GetPtrType(CodegenAnyVal::GetUnloweredType(codegen, TYPE_STRING));
      Value* dst_unlowered_ptr =
          builder.CreateBitCast(dst_lowered_ptr, unlowered_ptr_type, "dst_unlowered_ptr");

      // Call 'hll_fn'
      builder.CreateCall3(hll_fn, fn_ctx_arg, src_unlowered_ptr, dst_unlowered_ptr);

      // Convert StringVal intermediate 'dst_arg' back to StringValue
      Value* anyval_result = builder.CreateLoad(dst_lowered_ptr, "anyval_result");
      result = CodegenAnyVal(codegen, &builder, TYPE_STRING, anyval_result)
               .ToNativeValue();
      break;
    }
    default:
      DCHECK(false) << "bad aggregate operator: " << evaluator->agg_op();
  }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  builder.CreateStore(result, dst_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

<<<<<<< HEAD
// IR codegen for the UpdateAggTuple loop.  This loop is query specific and
// based on the aggregate exprs.  The function signature must match the non-
// codegen'd UpdateAggTuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @UpdateAggTuple(%"class.impala::AggregationNode"* %this_ptr,
//                             %"class.impala::AggregationTuple"* %agg_tuple, 
//                             %"class.impala::TupleRow"* %tuple_row) {
// entry:
//   %tuple = bitcast %"class.impala::AggregationNode"* %agg_tuple to 
//                                              { i8, i64, i64, double }*
//   %row = bitcast %"class.impala::TupleRow"* %tuple_row to i8**
//   %src_slot = getelementptr inbounds { i8, i64, i64, double }* %tuple, i32 0, i32 2
//   %count_star_val = load i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   call void @UpdateSlot({ i8, i64, i64, double }* %tuple, i8** %row)
//   call void @UpdateSlot2({ i8, i64, i64, double }* %tuple, i8** %row)
//   ret void
// }
Function* AggregationNode::CodegenUpdateAggTuple(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());
  
  for (int i = 0; i < probe_exprs_.size(); ++i) {
    if (probe_exprs_[i]->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because "
                 << "codegen for the grouping exprs is not yet supported.";
      return NULL;
    }
  }

  // string and timestamp aggregation currently not supported
  for (vector<Expr*>::const_iterator expr = aggregate_exprs_.begin();
      expr != aggregate_exprs_.end(); ++expr) {
    if ((*expr)->type() == TYPE_STRING || (*expr)->type() == TYPE_TIMESTAMP) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because "
                 << "string and timestamp aggregation is not yet supported.";
      return NULL;
    }
  } 

  for (int i = 0; i < aggregate_exprs_.size(); ++i) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    // If the agg_expr can't be generated, bail generating this function
    if (!agg_expr->is_star() && agg_expr->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because the "
                 << "underlying exprs cannot be codegened.";
      return NULL;
    }
    // Don't code gen distinct estiamte
    if (agg_expr->agg_op() == TAggregationOp::DISTINCT_PC
        || agg_expr->agg_op() == TAggregationOp::DISTINCT_PCSA
        || agg_expr->agg_op() == TAggregationOp::MERGE_PCSA
        || agg_expr->agg_op() == TAggregationOp::MERGE_PC) {
      return NULL;
    }
  }
  
  if (agg_tuple_desc_->GenerateLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateAggTuple because we could"
               << "not generate a  matching llvm struct for the result agg tuple.";
    return NULL;
  }

  // Get the types to match the UpdateAggTuple signature
  Type* agg_node_type = codegen->GetType(AggregationNode::LLVM_CLASS_NAME);
  Type* agg_tuple_type = codegen->GetType(AggregationTuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  
  DCHECK(agg_node_type != NULL);
  DCHECK(agg_tuple_type != NULL);
  DCHECK(tuple_row_type != NULL);
  
=======
// IR codegen for the UpdateTuple loop.  This loop is query specific and
// based on the aggregate functions.  The function signature must match the non-
// codegen'd UpdateTuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @UpdateTuple(%"class.impala::AggregationNode"* %this_ptr,
//                          %"class.impala::Tuple"* %agg_tuple,
//                          %"class.impala::TupleRow"* %tuple_row) #20 {
// entry:
//   %tuple = bitcast %"class.impala::Tuple"* %agg_tuple to { i8, i64, i64, double }*
//   %src_slot = getelementptr inbounds { i8, i64, i64, double }* %tuple, i32 0, i32 1
//   %count_star_val = load i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   call void @UpdateSlot(%"class.impala_udf::FunctionContext"* inttoptr
//                           (i64 44521296 to %"class.impala_udf::FunctionContext"*),
//                         { i8, i64, i64, double }* %tuple,
//                         %"class.impala::TupleRow"* %tuple_row)
//   call void @UpdateSlot5(%"class.impala_udf::FunctionContext"* inttoptr
//                            (i64 44521328 to %"class.impala_udf::FunctionContext"*),
//                          { i8, i64, i64, double }* %tuple,
//                          %"class.impala::TupleRow"* %tuple_row)
//   ret void
// }
Function* AggregationNode::CodegenUpdateTuple(RuntimeState* state) {
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;
  SCOPED_TIMER(codegen->codegen_timer());

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];

    // Timestamp and char are never supported. NDV supports decimal and string but no
    // other functions.
    // TODO: the other aggregate functions might work with decimal as-is
    if (slot_desc->type().type == TYPE_TIMESTAMP || slot_desc->type().type == TYPE_CHAR ||
        (evaluator->agg_op() != AggFnEvaluator::NDV &&
         (slot_desc->type().type == TYPE_DECIMAL ||
          slot_desc->type().type == TYPE_STRING ||
          slot_desc->type().type == TYPE_VARCHAR))) {
      VLOG_QUERY << "Could not codegen UpdateIntermediateTuple because "
                 << "string, char, timestamp and decimal are not yet supported.";
      return NULL;
    }

    // Don't codegen things that aren't builtins (for now)
    if (!evaluator->is_builtin()) return NULL;
  }

  if (intermediate_tuple_desc_->GenerateLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateTuple because we could"
               << "not generate a matching llvm struct for the intermediate tuple.";
    return NULL;
  }

  // Get the types to match the UpdateTuple signature
  Type* agg_node_type = codegen->GetType(AggregationNode::LLVM_CLASS_NAME);
  Type* agg_tuple_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);

  DCHECK(agg_node_type != NULL);
  DCHECK(agg_tuple_type != NULL);
  DCHECK(tuple_row_type != NULL);

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  PointerType* agg_node_ptr_type = PointerType::get(agg_node_type, 0);
  PointerType* agg_tuple_ptr_type = PointerType::get(agg_tuple_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

<<<<<<< HEAD
  // Signature for UpdateAggTuple is
  // void UpdateAggTuple(AggTuple* agg_tuple, char** row)
  // This signature needs to match the non-codegen'd signature exactly.
  PointerType* ptr_type = codegen->ptr_type();
  StructType* tuple_struct = agg_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateAggTuple", codegen->void_type());
=======
  // Signature for UpdateTuple is
  // void UpdateTuple(AggregationNode* this, Tuple* tuple, TupleRow* row)
  // This signature needs to match the non-codegen'd signature exactly.
  StructType* tuple_struct = intermediate_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateTuple", codegen->void_type());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", agg_tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  // Cast the parameter types to the internal llvm runtime types.
<<<<<<< HEAD
  args[1] = builder.CreateBitCast(args[1], tuple_ptr, "tuple");
  args[2] = builder.CreateBitCast(args[2], PointerType::get(ptr_type, 0), "row");

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  for (int i = 0; i < aggregate_exprs_.size(); ++i) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[probe_exprs_.size() + i];
    if (agg_expr->is_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      DCHECK_EQ(agg_expr->agg_op(), TAggregationOp::COUNT);
=======
  // TODO: get rid of this by using right type in function signature
  args[1] = builder.CreateBitCast(args[1], tuple_ptr, "tuple");

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!intermediate_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, intermediate_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    if (evaluator->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      int field_idx = slot_desc->field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(args[1], field_idx, "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
<<<<<<< HEAD
      Function* update_slot_fn = CodegenUpdateSlot(codegen, i);
      if (update_slot_fn == NULL) return NULL;
      builder.CreateCall2(update_slot_fn, args[1], args[2]);
=======
      Function* update_slot_fn = CodegenUpdateSlot(state, evaluator, slot_desc);
      if (update_slot_fn == NULL) return NULL;
      Value* fn_ctx_arg = codegen->CastPtrToLlvmPtr(
          codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME),
          agg_fn_ctxs_[i]);
      builder.CreateCall3(update_slot_fn, fn_ctx_arg, args[1], args[2]);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
  builder.CreateRetVoid();

<<<<<<< HEAD
  return codegen->OptimizeFunctionWithExprs(fn);
}

Function* AggregationNode::CodegenProcessRowBatch(
    LlvmCodeGen* codegen, Function* update_tuple_fn) {
=======
  // CodegenProcessRowBatch() does the final optimizations.
  return codegen->FinalizeFunction(fn);
}

Function* AggregationNode::CodegenProcessRowBatch(
    RuntimeState* state, Function* update_tuple_fn) {
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(update_tuple_fn != NULL);

  // Get the cross compiled update row batch function
<<<<<<< HEAD
  IRFunction::Type ir_fn = (singleton_output_tuple_ == NULL ?
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING :
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn);
  
=======
  IRFunction::Type ir_fn = (!probe_expr_ctxs_.empty() ?
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING :
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn);

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  if (process_batch_fn == NULL) {
    LOG(ERROR) << "Could not find AggregationNode::ProcessRowBatch in module.";
    return NULL;
  }
<<<<<<< HEAD
    
  int replaced = 0;
  if (singleton_output_tuple_ == NULL) {
    // Aggregation w/o grouping does not use a hash table.

    // Codegen for hash
    Function* hash_fn = hash_tbl_->CodegenHashCurrentRow(codegen);
    if (hash_fn == NULL) return NULL;
    
    // Codegen HashTable::Equals
    Function* equals_fn = hash_tbl_->CodegenEquals(codegen);
    if (equals_fn == NULL) return NULL;
    
    // Codegen for evaluating build rows
    Function* eval_build_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, true);
    if (eval_build_row_fn == NULL) return NULL;

    // Codegen for evaluating probe rows
    Function* eval_probe_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, false);
=======

  int replaced = 0;
  if (!probe_expr_ctxs_.empty()) {
    // Aggregation w/o grouping does not use a hash table.

    // Codegen for hash
    Function* hash_fn = hash_tbl_->CodegenHashCurrentRow(state);
    if (hash_fn == NULL) return NULL;

    // Codegen HashTable::Equals
    Function* equals_fn = hash_tbl_->CodegenEquals(state);
    if (equals_fn == NULL) return NULL;

    // Codegen for evaluating build rows
    Function* eval_build_row_fn = hash_tbl_->CodegenEvalTupleRow(state, true);
    if (eval_build_row_fn == NULL) return NULL;

    // Codegen for evaluating probe rows
    Function* eval_probe_row_fn = hash_tbl_->CodegenEvalTupleRow(state, false);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    if (eval_probe_row_fn == NULL) return NULL;

    // Replace call sites
    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_build_row_fn, "EvalBuildRow", &replaced);
    DCHECK_EQ(replaced, 1);
<<<<<<< HEAD
    
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_probe_row_fn, "EvalProbeRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        hash_fn, "HashCurrentRow", &replaced);
    DCHECK_EQ(replaced, 2);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        equals_fn, "Equals", &replaced);
    DCHECK_EQ(replaced, 1);
  }
<<<<<<< HEAD
  
  process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false, 
      update_tuple_fn, "UpdateAggTuple", &replaced); 
  DCHECK_EQ(replaced, 1) << "One call site should be replaced."; 
  DCHECK(process_batch_fn != NULL);

  return codegen->OptimizeFunctionWithExprs(process_batch_fn);
}

void AggregationNode::ConstructDistinctEstimateSlot(AggregationTuple* agg_tuple,
    const NullIndicatorOffset& null_indicator_offset, int string_slot_idx,
    void* slot) {
  // Initialize the distinct estimate bit map - Probablistic Counting Algorithms for Data
  // Base Applications (Flajolet and Martin)
  //
  // The bitmap is a 64bit(1st index) x 32bit(2nd index) matrix.
  // So, the string length of 256 byte is enough.
  // The layout is:
  //   row  1: 8bit 8bit 8bit 8bit
  //   row  2: 8bit 8bit 8bit 8bit
  //   ...     ..
  //   ...     ..
  //   row 64: 8bit 8bit 8bit 8bit
  //
  // Using 32bit length, we can count up to 10^8. This will not be enough for Fact table
  // primary key, but once we approach the limit, we could interpret the result as
  // "every row is distinct".
  //
  // We use "string" type for DISTINCT_PC function so that we can use the string
  // slot to hold the bitmaps.
  StringValue* bitmap_value = static_cast<StringValue*>(slot);
  int32_t* string_buffer_lengths = agg_tuple->BufferLengths(
      agg_tuple_desc_->byte_size());
  DCHECK_EQ(string_buffer_lengths[string_slot_idx], 0);

  int str_length = NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8;
  bitmap_value->ptr = AllocateStringBuffer(str_length,
      &(string_buffer_lengths[string_slot_idx]));
  bitmap_value->len = str_length;
  memset(bitmap_value->ptr, 0, str_length);
  agg_tuple->tuple()->SetNotNull(null_indicator_offset);
}

static inline void SetDistinctEstimateBit(char* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  // We need to convert Bitmap[alpha,index] into the index of the string.
  // alpha tells which of the 32bit we've to jump to.
  // index then lead us to the byte and bit.
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  int_bitmap[row_index] |= (1 << bit_index);
}

inline void AggregationNode::UpdateDistinctEstimatePCSASlot(void* slot, void* value,
    PrimitiveType type) {
  DCHECK(value != NULL);
  StringValue* dst_value = static_cast<StringValue*>(slot);

  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. Using stochastic averaging, we only need to
  // the hash value once for each row.
  uint32_t hash_value = RawValue::GetHashValue(value, type, 0);
  uint32_t row_index = hash_value % NUM_PC_BITMAPS;

  // We want the zero-based position of the least significant 1-bit in binary
  // representation of hash_value. __builtin_ctz does exactly this because it returns
  // the number of trailing 0-bits in x (or undefined if x is zero).
  int bit_index = __builtin_ctz(hash_value / NUM_PC_BITMAPS);
  if (UNLIKELY(hash_value == 0)) bit_index = PC_BITMAP_LENGTH - 1;

  // Set bitmap[row_index, bit_index] to 1
  SetDistinctEstimateBit(dst_value->ptr, row_index, bit_index);
}

inline void AggregationNode::UpdateDistinctEstimateSlot(void* slot, void* value,
    PrimitiveType type) {
  DCHECK(value != NULL);
  StringValue* dst_value = static_cast<StringValue*>(slot);

  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. For simple averaging, we need to compute hash
  // values NUM_PC_BITMAPS times using NUM_PC_BITMAPS different hash functions (by using a
  // different seed).
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    uint32_t hash_value = RawValue::GetHashValue(value, type, i);
    int bit_index = __builtin_ctz(hash_value);
    if (UNLIKELY(hash_value == 0)) bit_index = PC_BITMAP_LENGTH - 1;

    // Set bitmap[i, bit_index] to 1
    SetDistinctEstimateBit(dst_value->ptr, i, bit_index);
  }
}

inline void AggregationNode::UpdateMergeEstimateSlot(AggregationTuple* agg_tuple,
    int string_slot_idx, void* slot, void* value) {
  DCHECK(value != NULL);
  StringValue* dst_value = static_cast<StringValue*>(slot);
  StringValue* src_value = static_cast<StringValue*>(value);

  DCHECK_EQ(src_value->len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);

  int32_t* string_buffer_lengths = agg_tuple->BufferLengths(agg_tuple_desc_->byte_size());
  DCHECK_EQ(string_buffer_lengths[string_slot_idx],
      NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);

  // Merge the bits
  // I think _mm_or_ps can do it, but perf doesn't really matter here. We call this only
  // once group per node.
  for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
    *(dst_value->ptr + i) |= *(src_value->ptr + i);
  }

  VLOG_ROW << "UpdateMergeEstimateSlot Src Bit map:\n"
           << DistinctEstimateBitMapToString(src_value->ptr);
  VLOG_ROW << "UpdateMergeEstimateSlot Dst Bit map:\n"
           << DistinctEstimateBitMapToString(dst_value->ptr);
}

static inline bool GetDistinctEstimateBit(char* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  return ((int_bitmap[row_index] & (1 << bit_index)) > 0);
}

void AggregationNode::FinalizeEstimateSlot(int slot_id, void* slot,
    TAggregationOp::type agg_op) {
  StringValue* dst_value = static_cast<StringValue*>(slot);

  DCHECK_EQ(dst_value->len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);
  DCHECK(agg_op == TAggregationOp::DISTINCT_PCSA ||
      agg_op == TAggregationOp::MERGE_PCSA ||
      agg_op == TAggregationOp::DISTINCT_PC ||
      agg_op == TAggregationOp::MERGE_PC);
  VLOG_ROW << "FinalizeEstimateSlot Bit map:\n"
           << DistinctEstimateBitMapToString(dst_value->ptr);

  // We haven't processed any rows if none of the bits are set. Therefore, we have zero
  // distinct rows. We're overwriting the result in the same string buffer we've
  // allocated.
  bool is_empty = true;
  for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
    if (dst_value->ptr[i] != 0) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) {
    *(dst_value->ptr) = '0';
    dst_value->len = 1;
    return;
  }

  // Convert the bitmap to a number, please see the paper for details
  // In short, we count the average number of leading 1s (per row) in the bit map.
  // The number is proportional to the log2(1/NUM_PC_BITMAPS of  the actual number of
  // distinct).
  // To get the actual number of distinct, we'll do 2^avg / PC_THETA.
  // PC_THETA is a magic number.
  int sum = 0;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    int row_bit_count = 0;
    // Count the number of leading ones for each row in the bitmap
    // We could have used the build in __builtin_clz to count of number of leading zeros
    // but we first need to invert the 1 and 0.
    while (GetDistinctEstimateBit(dst_value->ptr, i, row_bit_count) &&
        row_bit_count < PC_BITMAP_LENGTH) {
      ++row_bit_count;
    }
    sum += row_bit_count;
  }
  double avg = static_cast<double>(sum) / static_cast<double>(NUM_PC_BITMAPS);
  double result = pow(static_cast<double>(2), avg) / PC_THETA;

  // If we're using stochastic averaging, the result has to be multiplied by
  // NUM_PC_BITMAPS.
  if (agg_op == TAggregationOp::DISTINCT_PCSA ||
      agg_op == TAggregationOp::MERGE_PCSA) {
    result *= NUM_PC_BITMAPS;
  }

  // We're overwriting the result ing the same string buffer we've allocated.
  stringstream out;
  out << static_cast<int>(result);
  strncpy(dst_value->ptr, out.str().c_str(), out.str().length());
  dst_value->len = out.str().length();
}

string AggregationNode::DistinctEstimateBitMapToString(char* v) {
  stringstream debugstr;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    for (int j = 0; j < PC_BITMAP_LENGTH; ++j) {
      // print bitmap[i][j]
      debugstr << GetDistinctEstimateBit(v, i, j);
    }
    debugstr << "\n";
  }
  debugstr << "\n";
  return debugstr.str();
}

}

=======

  process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
      update_tuple_fn, "UpdateTuple", &replaced);
  DCHECK_EQ(replaced, 1) << "One call site should be replaced.";
  DCHECK(process_batch_fn != NULL);
  return codegen->OptimizeFunctionWithExprs(process_batch_fn);
}

}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
