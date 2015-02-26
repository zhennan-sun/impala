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

#include "exec/exec-node.h"

#include <sstream>
<<<<<<< HEAD

=======
#include <unistd.h>  // for sleep()

#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exec/aggregation-node.h"
<<<<<<< HEAD
#include "exec/hash-join-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/exchange-node.h"
#include "exec/merge-node.h"
#include "exec/topn-node.h"
#include "runtime/descriptors.h"
=======
#include "exec/analytic-eval-node.h"
#include "exec/cross-join-node.h"
#include "exec/data-source-scan-node.h"
#include "exec/empty-set-node.h"
#include "exec/exchange-node.h"
#include "exec/hash-join-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/select-node.h"
#include "exec/partitioned-aggregation-node.h"
#include "exec/partitioned-hash-join-node.h"
#include "exec/sort-node.h"
#include "exec/topn-node.h"
#include "exec/union-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

using namespace llvm;
using namespace std;
using namespace boost;

<<<<<<< HEAD
=======
// TODO: remove when we remove hash-join-node.cc and aggregation-node.cc
DEFINE_bool(enable_partitioned_hash_join, true, "Enable partitioned hash join");
DEFINE_bool(enable_partitioned_aggregation, true, "Enable partitioned hash agg");

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
namespace impala {

const string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::GetNodeIdFromProfile(RuntimeProfile* p) {
  return p->metadata();
}

<<<<<<< HEAD
=======
ExecNode::RowBatchQueue::RowBatchQueue(int max_batches) :
    BlockingQueue<RowBatch*>(max_batches) {
}

ExecNode::RowBatchQueue::~RowBatchQueue() {
  DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(RowBatch* batch) {
  if (!BlockingPut(batch)) {
    ScopedSpinLock l(&lock_);
    cleanup_queue_.push_back(batch);
  }
}

RowBatch* ExecNode::RowBatchQueue::GetBatch() {
  RowBatch* result = NULL;
  if (BlockingGet(&result)) return result;
  return NULL;
}

int ExecNode::RowBatchQueue::Cleanup() {
  int num_io_buffers = 0;

  RowBatch* batch = NULL;
  while ((batch = GetBatch()) != NULL) {
    num_io_buffers += batch->num_io_buffers();
    delete batch;
  }

  ScopedSpinLock l(&lock_);
  for (list<RowBatch*>::iterator it = cleanup_queue_.begin();
      it != cleanup_queue_.end(); ++it) {
    num_io_buffers += (*it)->num_io_buffers();
    delete *it;
  }
  cleanup_queue_.clear();
  return num_io_buffers;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : id_(tnode.node_id),
    type_(tnode.node_type),
    pool_(pool),
    row_descriptor_(descs, tnode.row_tuples, tnode.nullable_tuples),
<<<<<<< HEAD
    limit_(tnode.limit),
    num_rows_returned_(0) {
  Status status = Expr::CreateExprTrees(pool, tnode.conjuncts, &conjuncts_);
  DCHECK(status.ok())
      << "ExecNode c'tor: deserialization of conjuncts failed:\n"
      << status.GetErrorMsg();
  InitRuntimeProfile(PrintPlanNodeType(tnode.node_type));
}

Status ExecNode::Prepare(RuntimeState* state) {
  DCHECK(runtime_profile_.get() != NULL);
  rows_returned_counter_ =
      ADD_COUNTER(runtime_profile_, "RowsReturned", TCounterType::UNIT);
  memory_used_counter_ =
      ADD_COUNTER(runtime_profile_, "MemoryUsed", TCounterType::BYTES);
  rows_returned_rate_ = runtime_profile()->AddDerivedCounter(
      ROW_THROUGHPUT_COUNTER, TCounterType::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, rows_returned_counter_, 
        runtime_profile()->total_time_counter()));


  RETURN_IF_ERROR(PrepareConjuncts(state));
=======
    debug_phase_(TExecNodePhase::INVALID),
    debug_action_(TDebugAction::WAIT),
    limit_(tnode.limit),
    num_rows_returned_(0),
    rows_returned_counter_(NULL),
    rows_returned_rate_(NULL),
    is_closed_(false) {
  InitRuntimeProfile(PrintPlanNodeType(tnode.node_type));
}

ExecNode::~ExecNode() {
}

Status ExecNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.conjuncts, &conjunct_ctxs_));
  return Status::OK;
}

Status ExecNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::PREPARE, state));
  DCHECK(runtime_profile_.get() != NULL);
  rows_returned_counter_ =
      ADD_COUNTER(runtime_profile_, "RowsReturned", TCounterType::UNIT);
  mem_tracker_.reset(new MemTracker(
      runtime_profile_.get(), -1, -1, runtime_profile_->name(),
      state->instance_mem_tracker()));
  expr_mem_tracker_.reset(new MemTracker(-1, -1, "Exprs", mem_tracker_.get(), false));

  rows_returned_rate_ = runtime_profile()->AddDerivedCounter(
      ROW_THROUGHPUT_COUNTER, TCounterType::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, rows_returned_counter_,
        runtime_profile()->total_time_counter()));

  RETURN_IF_ERROR(Expr::Prepare(conjunct_ctxs_, state, row_desc(), expr_mem_tracker()));
  AddExprCtxsToFree(conjunct_ctxs_);

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state));
  }
  return Status::OK;
}

<<<<<<< HEAD
Status ExecNode::Close(RuntimeState* state) {
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  Status result;
  for (int i = 0; i < children_.size(); ++i) {
    result.AddError(children_[i]->Close(state));
  }
  return result;
=======
Status ExecNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  return Expr::Open(conjunct_ctxs_, state);
}

void ExecNode::Close(RuntimeState* state) {
  if (is_closed_) return;
  is_closed_ = true;

  if (rows_returned_counter_ != NULL) {
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state);
  }
  Expr::Close(conjunct_ctxs_, state);

  if (mem_tracker() != NULL) {
    if (mem_tracker()->consumption() != 0) {
      LOG(WARNING) << "Query " << state->query_id() << " leaked memory." << endl
          << state->instance_mem_tracker()->LogUsage();
      DCHECK_EQ(mem_tracker()->consumption(), 0)
          << "Leaked memory." << endl << state->instance_mem_tracker()->LogUsage();
    }
  }
}

void ExecNode::AddRuntimeExecOption(const string& str) {
  lock_guard<mutex> l(exec_options_lock_);
  if (runtime_exec_options_.empty()) {
    runtime_exec_options_ = str;
  } else {
    runtime_exec_options_.append(", ");
    runtime_exec_options_.append(str);
  }
  runtime_profile()->AddInfoString("ExecOption", runtime_exec_options_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

Status ExecNode::CreateTree(ObjectPool* pool, const TPlan& plan,
                            const DescriptorTbl& descs, ExecNode** root) {
  if (plan.nodes.size() == 0) {
    *root = NULL;
    return Status::OK;
  }
  int node_idx = 0;
<<<<<<< HEAD
  RETURN_IF_ERROR(CreateTreeHelper(pool, plan.nodes, descs, NULL, &node_idx, root));
  if (node_idx + 1 != plan.nodes.size()) {
    // TODO: print thrift msg for diagnostic purposes.
    return Status(
        "Plan tree only partially reconstructed. Not all thrift nodes were used.");
  }
  return Status::OK;
=======
  Status status = CreateTreeHelper(pool, plan.nodes, descs, NULL, &node_idx, root);
  if (status.ok() && node_idx + 1 != plan.nodes.size()) {
    status = Status(
        "Plan tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct plan tree:\n"
               << apache::thrift::ThriftDebugString(plan);
  }
  return status;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

Status ExecNode::CreateTreeHelper(
    ObjectPool* pool,
    const vector<TPlanNode>& tnodes,
    const DescriptorTbl& descs,
    ExecNode* parent,
    int* node_idx,
    ExecNode** root) {
  // propagate error case
  if (*node_idx >= tnodes.size()) {
<<<<<<< HEAD
    // TODO: print thrift msg
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return Status("Failed to reconstruct plan tree from thrift.");
  }
  int num_children = tnodes[*node_idx].num_children;
  ExecNode* node = NULL;
  RETURN_IF_ERROR(CreateNode(pool, tnodes[*node_idx], descs, &node));
  // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
  if (parent != NULL) {
    parent->children_.push_back(node);
  } else {
    *root = node;
  }
<<<<<<< HEAD
  for (int i = 0; i < num_children; i++) {
=======
  for (int i = 0; i < num_children; ++i) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    ++*node_idx;
    RETURN_IF_ERROR(CreateTreeHelper(pool, tnodes, descs, node, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= tnodes.size()) {
<<<<<<< HEAD
      // TODO: print thrift msg
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return Status("Failed to reconstruct plan tree from thrift.");
    }
  }

  // build up tree of profiles; add children >0 first, so that when we print
  // the profile, child 0 is printed last (makes the output more readable)
  for (int i = 1; i < node->children_.size(); ++i) {
    node->runtime_profile()->AddChild(node->children_[i]->runtime_profile());
  }
  if (!node->children_.empty()) {
    node->runtime_profile()->AddChild(node->children_[0]->runtime_profile(), false);
  }

  return Status::OK;
}

Status ExecNode::CreateNode(ObjectPool* pool, const TPlanNode& tnode,
                            const DescriptorTbl& descs, ExecNode** node) {
  stringstream error_msg;
  switch (tnode.node_type) {
    case TPlanNodeType::HDFS_SCAN_NODE:
      *node = pool->Add(new HdfsScanNode(pool, tnode, descs));
<<<<<<< HEAD
      return Status::OK;
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::AGGREGATION_NODE:
      *node = pool->Add(new AggregationNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::HASH_JOIN_NODE:
      *node = pool->Add(new HashJoinNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangeNode(pool, tnode, descs));
      return Status::OK;
=======
      break;
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::DATA_SOURCE_NODE:
      *node = pool->Add(new DataSourceScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::AGGREGATION_NODE:
      if (FLAGS_enable_partitioned_aggregation) {
        *node = pool->Add(new PartitionedAggregationNode(pool, tnode, descs));
      } else {
        *node = pool->Add(new AggregationNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::HASH_JOIN_NODE:
      // The (old) HashJoinNode does not support left-anti, right-semi, and right-anti
      // joins.
      if (tnode.hash_join_node.join_op == TJoinOp::LEFT_ANTI_JOIN ||
          tnode.hash_join_node.join_op == TJoinOp::RIGHT_SEMI_JOIN ||
          tnode.hash_join_node.join_op == TJoinOp::RIGHT_ANTI_JOIN ||
          FLAGS_enable_partitioned_hash_join) {
        *node = pool->Add(new PartitionedHashJoinNode(pool, tnode, descs));
      } else {
        *node = pool->Add(new HashJoinNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::CROSS_JOIN_NODE:
      *node = pool->Add(new CrossJoinNode(pool, tnode, descs));
      break;
    case TPlanNodeType::EMPTY_SET_NODE:
      *node = pool->Add(new EmptySetNode(pool, tnode, descs));
      break;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangeNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SELECT_NODE:
      *node = pool->Add(new SelectNode(pool, tnode, descs));
      break;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    case TPlanNodeType::SORT_NODE:
      if (tnode.sort_node.use_top_n) {
        *node = pool->Add(new TopNNode(pool, tnode, descs));
      } else {
<<<<<<< HEAD
      // TODO: Need Sort Node
      //  *node = pool->Add(new SortNode(pool, tnode, descs));
        error_msg << "ORDER BY with no LIMIT not implemented";
        return Status(error_msg.str());
      }
      return Status::OK;
    case TPlanNodeType::MERGE_NODE:
      *node = pool->Add(new MergeNode(pool, tnode, descs));
      return Status::OK;
=======
        *node = pool->Add(new SortNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::UNION_NODE:
      *node = pool->Add(new UnionNode(pool, tnode, descs));
      break;
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
      *node = pool->Add(new AnalyticEvalNode(pool, tnode, descs));
      break;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    default:
      map<int, const char*>::const_iterator i =
          _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
      const char* str = "unknown node type";
      if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented";
      return Status(error_msg.str());
  }
<<<<<<< HEAD
  return Status::OK;
}

=======
  RETURN_IF_ERROR((*node)->Init(tnode));
  return Status::OK;
}

void ExecNode::SetDebugOptions(
    int node_id, TExecNodePhase::type phase, TDebugAction::type action,
    ExecNode* root) {
  if (root->id_ == node_id) {
    root->debug_phase_ = phase;
    root->debug_action_ = action;
    return;
  }
  for (int i = 0; i < root->children_.size(); ++i) {
    SetDebugOptions(node_id, phase, action, root->children_[i]);
  }
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
string ExecNode::DebugString() const {
  stringstream out;
  this->DebugString(0, &out);
  return out.str();
}

void ExecNode::DebugString(int indentation_level, stringstream* out) const {
<<<<<<< HEAD
  *out << " conjuncts=" << Expr::DebugString(conjuncts_);
=======
  *out << " conjuncts=" << Expr::DebugString(conjunct_ctxs_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  for (int i = 0; i < children_.size(); ++i) {
    *out << "\n";
    children_[i]->DebugString(indentation_level + 1, out);
  }
}

<<<<<<< HEAD
Status ExecNode::PrepareConjuncts(RuntimeState* state) {
  for (vector<Expr*>::iterator i = conjuncts_.begin(); i != conjuncts_.end(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(*i, state, row_desc()));
=======
void ExecNode::CollectNodes(TPlanNodeType::type node_type, vector<ExecNode*>* nodes) {
  if (type_ == node_type) nodes->push_back(this);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->CollectNodes(node_type, nodes);
  }
}

void ExecNode::CollectScanNodes(vector<ExecNode*>* nodes) {
  CollectNodes(TPlanNodeType::HDFS_SCAN_NODE, nodes);
  CollectNodes(TPlanNodeType::HBASE_SCAN_NODE, nodes);
}

void ExecNode::InitRuntimeProfile(const string& name) {
  stringstream ss;
  ss << name << " (id=" << id_ << ")";
  runtime_profile_.reset(new RuntimeProfile(pool_, ss.str()));
  runtime_profile_->set_metadata(id_);
}

Status ExecNode::ExecDebugAction(TExecNodePhase::type phase, RuntimeState* state) {
  DCHECK(phase != TExecNodePhase::INVALID);
  if (debug_phase_ != phase) return Status::OK;
  if (debug_action_ == TDebugAction::FAIL) return Status(TStatusCode::INTERNAL_ERROR);
  if (debug_action_ == TDebugAction::WAIT) {
    while (!state->is_cancelled()) {
      sleep(1);
    }
    return Status::CANCELLED;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
bool ExecNode::EvalConjuncts(Expr* const* exprs, int num_exprs, TupleRow* row) {
  for (int i = 0; i < num_exprs; ++i) {
    void* value = exprs[i]->GetValue(row);
    if (value == NULL || *reinterpret_cast<bool*>(value) == false) return false;
=======
bool ExecNode::EvalConjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  for (int i = 0; i < num_ctxs; ++i) {
    BooleanVal v = ctxs[i]->GetBooleanVal(row);
    if (v.is_null || !v.val) return false;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return true;
}

<<<<<<< HEAD
// Codegen for EvalConjuncts.  The generated signature is
// For a node with two conjunct predicates
// define i1 @EvalConjuncts(%"class.impala::Expr"** %exprs, i32 %num_exprs, 
//                          %"class.impala::TupleRow"* %row) {
// entry:
//   %null_ptr = alloca i1
//   %0 = bitcast %"class.impala::TupleRow"* %row to i8**
//   %eval = call i1 @BinaryPredicate(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval, label %continue, label %false
// 
// continue:                                         ; preds = %entry
//   %eval2 = call i1 @BinaryPredicate3(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval2, label %continue1, label %false
// 
// continue1:                                        ; preds = %continue
//   ret i1 true
// 
// false:                                            ; preds = %continue, %entry
//   ret i1 false
// }
Function* ExecNode::CodegenEvalConjuncts(LlvmCodeGen* codegen,
    const vector<Expr*>& conjuncts) {
  for (int i = 0; i < conjuncts.size(); ++i) {
    if (conjuncts[i]->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen EvalConjuncts because one of the conjuncts "
                 << "could not be codegen'd.";
      return NULL;
    }
  }
=======
Status ExecNode::QueryMaintenance(RuntimeState* state) {
  ExprContext::FreeLocalAllocations(expr_ctxs_to_free_);
  return state->CheckQueryState();
}

void ExecNode::AddExprCtxsToFree(const vector<ExprContext*>& ctxs) {
  for (int i = 0; i < ctxs.size(); ++i) AddExprCtxToFree(ctxs[i]);
}

void ExecNode::AddExprCtxsToFree(const SortExecExprs& sort_exec_exprs) {
  AddExprCtxsToFree(sort_exec_exprs.sort_tuple_slot_expr_ctxs());
  AddExprCtxsToFree(sort_exec_exprs.lhs_ordering_expr_ctxs());
  AddExprCtxsToFree(sort_exec_exprs.rhs_ordering_expr_ctxs());
}

// Codegen for EvalConjuncts.  The generated signature is
// For a node with two conjunct predicates
// define i1 @EvalConjuncts(%"class.impala::ExprContext"** %ctxs, i32 %num_ctxs,
//                          %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %ctx_ptr = getelementptr %"class.impala::ExprContext"** %ctxs, i32 0
//   %ctx = load %"class.impala::ExprContext"** %ctx_ptr
//   %result = call i16 @Eq_StringVal_StringValWrapper3(
//       %"class.impala::ExprContext"* %ctx, %"class.impala::TupleRow"* %row)
//   %is_null = trunc i16 %result to i1
//   %0 = ashr i16 %result, 8
//   %1 = trunc i16 %0 to i8
//   %val = trunc i8 %1 to i1
//   %is_false = xor i1 %val, true
//   %return_false = or i1 %is_null, %is_false
//   br i1 %return_false, label %false, label %continue
//
// continue:                                         ; preds = %entry
//   %ctx_ptr2 = getelementptr %"class.impala::ExprContext"** %ctxs, i32 1
//   %ctx3 = load %"class.impala::ExprContext"** %ctx_ptr2
//   %result4 = call i16 @Gt_BigIntVal_BigIntValWrapper5(
//       %"class.impala::ExprContext"* %ctx3, %"class.impala::TupleRow"* %row)
//   %is_null5 = trunc i16 %result4 to i1
//   %2 = ashr i16 %result4, 8
//   %3 = trunc i16 %2 to i8
//   %val6 = trunc i8 %3 to i1
//   %is_false7 = xor i1 %val6, true
//   %return_false8 = or i1 %is_null5, %is_false7
//   br i1 %return_false8, label %false, label %continue1
//
// continue1:                                        ; preds = %continue
//   ret i1 true
//
// false:                                            ; preds = %continue, %entry
//   ret i1 false
// }
Function* ExecNode::CodegenEvalConjuncts(
    RuntimeState* state, const vector<ExprContext*>& conjunct_ctxs, const char* name) {
  Function* conjunct_fns[conjunct_ctxs.size()];
  for (int i = 0; i < conjunct_ctxs.size(); ++i) {
    Status status =
        conjunct_ctxs[i]->root()->GetCodegendComputeFn(state, &conjunct_fns[i]);
    if (!status.ok()) {
      VLOG_QUERY << "Could not codegen EvalConjuncts: " << status.GetErrorMsg();
      return NULL;
    }
  }
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Construct function signature to match
  // bool EvalConjuncts(Expr** exprs, int num_exprs, TupleRow* row)
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
<<<<<<< HEAD
  Type* expr_type = codegen->GetType(Expr::LLVM_CLASS_NAME);

  DCHECK(tuple_row_type != NULL);
  DCHECK(expr_type != NULL);
  
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* expr_ptr_type = PointerType::get(expr_type, 0);

  LlvmCodeGen::FnPrototype prototype(
      codegen, "EvalConjuncts", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("exprs", PointerType::get(expr_ptr_type, 0)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("num_exprs", codegen->GetType(TYPE_INT)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  
  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  // Other args are unused. 
  Value* tuple_row_arg = args[2];

  if (conjuncts.size() > 0) {
    LLVMContext& context = codegen->context();
    // The exprs type TupleRows as char** (instead of TupleRow* or Tuple**).  We
    // could plumb the expr codegen to know the tuples it will operate on.
    // TODO: think about doing that
    Type* tuple_row_llvm_type = PointerType::get(codegen->ptr_type(), 0);
    tuple_row_arg = builder.CreateBitCast(tuple_row_arg, tuple_row_llvm_type);
    BasicBlock* false_block = BasicBlock::Create(context, "false", fn);
  
    LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
    Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

    for (int i = 0; i < conjuncts.size(); ++i) {
      BasicBlock* true_block = BasicBlock::Create(context, "continue", fn, false_block);
      Function* conjunct_fn = conjuncts[i]->codegen_fn();
      DCHECK_EQ(conjuncts[i]->scratch_buffer_size(), 0);
      Value* expr_args[] = { tuple_row_arg, codegen->null_ptr_value(), is_null_ptr };

      // Ignore null result.  If null, expr's will return false which
      // is exactly the semantics for conjuncts
      Value* eval = builder.CreateCall(conjunct_fn, expr_args, "eval");
      builder.CreateCondBr(eval, true_block, false_block);
=======
  Type* expr_ctx_type = codegen->GetType(ExprContext::LLVM_CLASS_NAME);

  DCHECK(tuple_row_type != NULL);
  DCHECK(expr_ctx_type != NULL);

  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* expr_ctx_ptr_type = PointerType::get(expr_ctx_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, name, codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("ctxs", PointerType::get(expr_ctx_ptr_type, 0)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("num_ctxs", codegen->GetType(TYPE_INT)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  Value* ctxs_arg = args[0];
  Value* tuple_row_arg = args[2];

  if (conjunct_ctxs.size() > 0) {
    LLVMContext& context = codegen->context();
    BasicBlock* false_block = BasicBlock::Create(context, "false", fn);

    for (int i = 0; i < conjunct_ctxs.size(); ++i) {
      BasicBlock* true_block = BasicBlock::Create(context, "continue", fn, false_block);

      Value* ctx_arg_ptr = builder.CreateConstGEP1_32(ctxs_arg, i, "ctx_ptr");
      Value* ctx_arg = builder.CreateLoad(ctx_arg_ptr, "ctx");
      Value* expr_args[] = { ctx_arg, tuple_row_arg };

      // Call conjunct_fns[i]
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
          codegen, &builder, conjunct_ctxs[i]->root()->type(), conjunct_fns[i], expr_args,
          "result");

      // Return false if result.is_null || !result
      Value* is_null = result.GetIsNull();
      Value* is_false = builder.CreateNot(result.GetVal(), "is_false");
      Value* return_false = builder.CreateOr(is_null, is_false, "return_false");
      builder.CreateCondBr(return_false, false_block, true_block);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

      // Set insertion point for continue/end
      builder.SetInsertPoint(true_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());
  } else {
    builder.CreateRet(codegen->true_value());
  }

  return codegen->FinalizeFunction(fn);
}

<<<<<<< HEAD
void ExecNode::CollectNodes(TPlanNodeType::type node_type, vector<ExecNode*>* nodes) {
  if (type_ == node_type) nodes->push_back(this);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->CollectNodes(node_type, nodes);
  }
}

void ExecNode::CollectScanNodes(vector<ExecNode*>* nodes) {
  CollectNodes(TPlanNodeType::HDFS_SCAN_NODE, nodes);
  CollectNodes(TPlanNodeType::HBASE_SCAN_NODE, nodes);
}

void ExecNode::InitRuntimeProfile(const string& name) {
  stringstream ss;
  ss << name << " (id=" << id_ << ")";
  runtime_profile_.reset(new RuntimeProfile(pool_, ss.str()));
  runtime_profile_->set_metadata(id_);
}

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
