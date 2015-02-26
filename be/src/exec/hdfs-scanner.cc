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

#include "exec/hdfs-scanner.h"

#include <sstream>
#include <boost/algorithm/string.hpp>

<<<<<<< HEAD
=======
#include "codegen/codegen-anyval.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/text-converter.h"
<<<<<<< HEAD
#include "exec/hdfs-byte-stream.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scan-range-context.h"
#include "exec/text-converter.inline.h"
#include "exprs/expr.h"
=======
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/text-converter.inline.h"
#include "exprs/expr-context.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
<<<<<<< HEAD
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
=======
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/codec.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "util/sse-util.h"
#include "util/string-parser.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* FieldLocation::LLVM_CLASS_NAME = "struct.impala::FieldLocation";
const char* HdfsScanner::LLVM_CLASS_NAME = "class.impala::HdfsScanner";

<<<<<<< HEAD
HdfsScanner::HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state, 
                         MemPool* tuple_pool)
    : scan_node_(scan_node),
      state_(state),
      context_(NULL),
      conjuncts_(NULL),
      num_conjuncts_(0),
      tuple_buffer_(NULL),
      tuple_byte_size_(scan_node->tuple_desc()->byte_size()),
      tuple_pool_(tuple_pool),
      tuple_(NULL),
      num_errors_in_file_(0),
      current_byte_stream_(NULL),
      template_tuple_(NULL),
      has_noncompact_strings_(!scan_node->compact_data() &&
                              !scan_node->tuple_desc()->string_slots().empty()),
      current_scan_range_(NULL),
      num_null_bytes_(scan_node->tuple_desc()->num_null_bytes()),
=======
HdfsScanner::HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : scan_node_(scan_node),
      state_(state),
      context_(NULL),
      tuple_byte_size_(scan_node->tuple_desc()->byte_size()),
      tuple_(NULL),
      batch_(NULL),
      num_errors_in_file_(0),
      num_null_bytes_(scan_node->tuple_desc()->num_null_bytes()),
      decompression_type_(THdfsCompression::NONE),
      data_buffer_pool_(new MemPool(scan_node->mem_tracker())),
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      write_tuples_fn_(NULL) {
}

HdfsScanner::~HdfsScanner() {
<<<<<<< HEAD
}

Status HdfsScanner::Prepare() {
  RETURN_IF_ERROR(CreateConjunctsCopy());
  return Status::OK;
}

Status HdfsScanner::CreateConjunctsCopy() {
  DCHECK(conjuncts_ == NULL);
  RETURN_IF_ERROR(scan_node_->CreateConjuncts(&conjuncts_mem_));
  if (conjuncts_mem_.size() > 0) {
    conjuncts_ = &conjuncts_mem_[0];
  }
  num_conjuncts_ = conjuncts_mem_.size();
  return Status::OK;
}

Status HdfsScanner::InitializeCodegenFn(HdfsPartitionDescriptor* partition,
    THdfsFileFormat::type type, const string& scanner_name) {
  Function* codegen_fn = scan_node_->GetCodegenFn(type);
  
  if (codegen_fn == NULL) return Status::OK;
  if (!scan_node_->tuple_desc()->string_slots().empty() && 
        ((partition->escape_char() != '\0') || context_->compact_data())) {
    // Cannot use codegen if there are strings slots and we need to 
    // compact (i.e. copy) the data.
    return Status::OK;
  }

  write_tuples_fn_ = reinterpret_cast<WriteTuplesFn>(
      state_->llvm_codegen()->JitFunction(codegen_fn));
  VLOG(2) << scanner_name << "(node_id=" << scan_node_->id() 
          << ") using llvm codegend functions.";
  return Status::OK;
}

void HdfsScanner::AllocateTupleBuffer(RowBatch* row_batch) {
  if (tuple_byte_size_ == 0) {
    tuple_byte_size_ = 0;
  } else if (tuple_ == NULL) {
    // create new tuple buffer for row_batch
    tuple_buffer_size_ = row_batch->capacity() * tuple_byte_size_;
    tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
    tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);
  }
}

Status HdfsScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    DiskIoMgr::ScanRange* range, Tuple* template_tuple, ByteStream* byte_stream) {
  // There is only one case where the scanner would get a new template_tuple
  // memory location.  Most of the time, the template_tuple is copied into
  // the tuple being materialized so the template tuple memory can be reused.  
  // However, if there are no materialized slots (partition key only), then the 
  // template tuple address is directly put into the tuplerows.  In this case we need
  // a new memory location for the template tuple.  The memory savings is
  // not important but the address of template_tuple is baked into the codegen
  // functions.  In the case with no materialized slots, there is no codegen
  // function so it is not a problem.
  DCHECK(byte_stream != NULL);
  current_scan_range_ = range;
  template_tuple_ = template_tuple;
  current_byte_stream_ = byte_stream;
  has_noncompact_strings_ = (!scan_node_->compact_data() &&
                             !scan_node_->tuple_desc()->string_slots().empty());
  return Status::OK;
}

=======
  DCHECK(batch_ == NULL);
}

Status HdfsScanner::Prepare(ScannerContext* context) {
  context_ = context;
  stream_ = context->GetStream();
  RETURN_IF_ERROR(scan_node_->GetConjunctCtxs(&conjunct_ctxs_));
  template_tuple_ = scan_node_->InitTemplateTuple(
      state_, context_->partition_descriptor()->partition_key_value_ctxs());
  StartNewRowBatch();
  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  return Status::OK;
}

void HdfsScanner::Close() {
  if (decompressor_.get() != NULL) decompressor_->Close();
  Expr::Close(conjunct_ctxs_, state_);
}

Status HdfsScanner::InitializeWriteTuplesFn(HdfsPartitionDescriptor* partition,
    THdfsFileFormat::type type, const string& scanner_name) {
  if (!scan_node_->tuple_desc()->string_slots().empty()
      && partition->escape_char() != '\0') {
    // Cannot use codegen if there are strings slots and we need to
    // compact (i.e. copy) the data.
    scan_node_->IncNumScannersCodegenDisabled();
    return Status::OK;
  }

  write_tuples_fn_ = reinterpret_cast<WriteTuplesFn>(scan_node_->GetCodegenFn(type));
  if (write_tuples_fn_ == NULL) {
    scan_node_->IncNumScannersCodegenDisabled();
    return Status::OK;
  }
  VLOG(2) << scanner_name << "(node_id=" << scan_node_->id()
          << ") using llvm codegend functions.";
  scan_node_->IncNumScannersCodegenEnabled();
  return Status::OK;
}

void HdfsScanner::StartNewRowBatch() {
  batch_ = new RowBatch(scan_node_->row_desc(), state_->batch_size(),
      scan_node_->mem_tracker());
  tuple_mem_ =
      batch_->tuple_data_pool()->Allocate(state_->batch_size() * tuple_byte_size_);
}

int HdfsScanner::GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem) {
  DCHECK(batch_ != NULL);
  DCHECK_GT(batch_->capacity(), batch_->num_rows());
  *pool = batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = batch_->GetRow(batch_->AddRow());
  return batch_->capacity() - batch_->num_rows();
}

Status HdfsScanner::CommitRows(int num_rows) {
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += scan_node_->tuple_desc()->byte_size() * num_rows;

  // We need to pass the row batch to the scan node if there is too much memory attached,
  // which can happen if the query is very selective.
  if (batch_->AtCapacity() || context_->num_completed_io_buffers() > 0) {
    context_->ReleaseCompletedResources(batch_, /* done */ false);
    scan_node_->AddMaterializedRowBatch(batch_);
    StartNewRowBatch();
  }

  if (context_->cancelled()) return Status::CANCELLED;
  RETURN_IF_ERROR(state_->CheckQueryState());
  // Free local expr allocations for this thread
  ExprContext::FreeLocalAllocations(conjunct_ctxs_);
  return Status::OK;
}

void HdfsScanner::AddFinalRowBatch() {
  DCHECK(batch_ != NULL);
  context_->ReleaseCompletedResources(batch_, /* done */ true);
  scan_node_->AddMaterializedRowBatch(batch_);
  batch_ = NULL;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
int HdfsScanner::WriteEmptyTuples(RowBatch* row_batch, int num_tuples) {
<<<<<<< HEAD
  // Cap the number of result tuples up at the limit
  if (scan_node_->limit() != -1) {
    num_tuples = min(num_tuples,
        static_cast<int>(scan_node_->limit() - scan_node_->rows_returned()));
  }
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  DCHECK_GT(num_tuples, 0);

  if (template_tuple_ == NULL) {
    // No slots from partitions keys or slots.  This is count(*).  Just add the
    // number of rows to the batch.
    row_batch->AddRows(num_tuples);
    row_batch->CommitRows(num_tuples);
  } else {
    // Make a row and evaluate the row
    int row_idx = row_batch->AddRow();
<<<<<<< HEAD
    
    TupleRow* current_row = row_batch->GetRow(row_idx);
    current_row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, current_row)) {
      return 0;
    }
=======

    TupleRow* current_row = row_batch->GetRow(row_idx);
    current_row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!EvalConjuncts(current_row)) return 0;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    // Add first tuple
    row_batch->CommitLastRow();
    --num_tuples;

    DCHECK_LE(num_tuples, row_batch->capacity() - row_batch->num_rows());

    for (int n = 0; n < num_tuples; ++n) {
<<<<<<< HEAD
      DCHECK(!row_batch->IsFull());
=======
      DCHECK(!row_batch->AtCapacity());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      row_idx = row_batch->AddRow();
      DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* current_row = row_batch->GetRow(row_idx);
      current_row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
      row_batch->CommitLastRow();
    }
<<<<<<< HEAD
  } 
=======
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return num_tuples;
}

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
<<<<<<< HEAD
int HdfsScanner::WriteEmptyTuples(ScanRangeContext* context, 
    TupleRow* row, int num_tuples) {
  // Cap the number of result tuples up at the limit
  if (scan_node_->limit() != -1) {
    num_tuples = min(num_tuples,
        static_cast<int>(scan_node_->limit() - scan_node_->rows_returned()));
  }
  if (num_tuples == 0) return 0;

  if (template_tuple_ == NULL) {
    return num_tuples;
  } else {
    row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, row)) return 0;
    row = context->next_row(row);

    for (int n = 1; n < num_tuples; ++n) {
      row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
      row = context->next_row(row);
    }
  } 
  return num_tuples;
}

bool HdfsScanner::WriteCompleteTuple(MemPool* pool, FieldLocation* fields, 
=======
int HdfsScanner::WriteEmptyTuples(ScannerContext* context,
    TupleRow* row, int num_tuples) {
  DCHECK_GE(num_tuples, 0);
  if (num_tuples == 0) return 0;

  if (template_tuple_ == NULL) {
    // Must be conjuncts on constant exprs.
    if (!EvalConjuncts(row)) return 0;
    return num_tuples;
  } else {
    row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!EvalConjuncts(row)) return 0;
    row = next_row(row);

    for (int n = 1; n < num_tuples; ++n) {
      row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
      row = next_row(row);
    }
  }
  return num_tuples;
}

bool HdfsScanner::WriteCompleteTuple(MemPool* pool, FieldLocation* fields,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    Tuple* tuple, TupleRow* tuple_row, Tuple* template_tuple,
    uint8_t* error_fields, uint8_t* error_in_row) {
  *error_in_row = false;
  // Initialize tuple before materializing slots
  InitTuple(template_tuple, tuple);

  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    int need_escape = false;
    int len = fields[i].len;
    if (UNLIKELY(len < 0)) {
      len = -len;
      need_escape = true;
    }

    SlotDescriptor* desc = scan_node_->materialized_slots()[i];
    bool error = !text_converter_->WriteSlot(desc, tuple,
<<<<<<< HEAD
        fields[i].start, len, context_->compact_data(), need_escape, pool);
=======
        fields[i].start, len, false, need_escape, pool);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    error_fields[i] = error;
    *error_in_row |= error;
  }

  tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);
<<<<<<< HEAD
  return ExecNode::EvalConjuncts(&conjuncts_mem_[0], num_conjuncts_, tuple_row);
=======
  return EvalConjuncts(tuple_row);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

// Codegen for WriteTuple(above).  The signature matches WriteTuple (except for the
// this* first argument).  For writing out and evaluating a single string slot:
<<<<<<< HEAD
// define i1 @WriteCompleteTuple(%"class.impala::HdfsTextScanner"* %this, 
//                               %"class.impala::MemPool"* %pool,
//                               %"struct.impala::FieldLocation"* %fields, 
//                               %"class.impala::Tuple"* %tuple, 
//                               %"class.impala::TupleRow"* %tuple_row, 
//                               %"class.impala::Tuple"* %template, 
//                               i8* %error_fields, i8* %error_in_row) {
// entry:
//   %null_ptr = alloca i1
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple 
//                                              to { i8, %"struct.impala::StringValue" }*
//   %tuple_row_ptr = bitcast %"class.impala::TupleRow"* %tuple_row to i8**
//   %null_byte = getelementptr inbounds 
//                    { i8, %"struct.impala::StringValue" }* %tuple_ptr, i32 0, i32 0
//   store i8 0, i8* %null_byte
//   %0 = bitcast i8** %tuple_row_ptr to { i8, %"struct.impala::StringValue" }**
//   %1 = getelementptr { i8, %"struct.impala::StringValue" }** %0, i32 0
//   store { i8, %"struct.impala::StringValue" }* %tuple_ptr, 
=======
// define i1 @WriteCompleteTuple(%"class.impala::HdfsScanner"* %this,
//                               %"class.impala::MemPool"* %pool,
//                               %"struct.impala::FieldLocation"* %fields,
//                               %"class.impala::Tuple"* %tuple,
//                               %"class.impala::TupleRow"* %tuple_row,
//                               %"class.impala::Tuple"* %template,
//                               i8* %error_fields, i8* %error_in_row) #20 {
// entry:
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple
//                to { i8, %"struct.impala::StringValue" }*
//   %tuple_ptr1 = bitcast %"class.impala::Tuple"* %template
//                 to { i8, %"struct.impala::StringValue" }*
//   %null_byte = getelementptr inbounds
//                { i8, %"struct.impala::StringValue" }* %tuple_ptr, i32 0, i32 0
//   store i8 0, i8* %null_byte
//   %0 = bitcast %"class.impala::TupleRow"* %tuple_row
//        to { i8, %"struct.impala::StringValue" }**
//   %1 = getelementptr { i8, %"struct.impala::StringValue" }** %0, i32 0
//   store { i8, %"struct.impala::StringValue" }* %tuple_ptr,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
//         { i8, %"struct.impala::StringValue" }** %1
//   br label %parse
// 
// parse:                                            ; preds = %entry
//   %data_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 0
//   %len_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 1
//   %slot_error_ptr = getelementptr i8* %error_fields, i32 0
//   %data = load i8** %data_ptr
//   %len = load i32* %len_ptr
<<<<<<< HEAD
//   %2 = call i1 @WriteSlot({ i8, %"struct.impala::StringValue" }* 
//                                 %tuple_ptr, i8* %data, i32 %len)
//   %slot_parse_error = xor i1 %2, true
//   %error_in_row1 = or i1 false, %slot_parse_error
//   %3 = zext i1 %slot_parse_error to i8
//   store i8 %3, i8* %slot_error_ptr
//   %conjunct_eval = call i1 @BinaryPredicate(i8** %tuple_row_ptr, 
//                                             i8* null, i1* %null_ptr)
//   br i1 %conjunct_eval, label %parse2, label %eval_fail
// 
// parse2:                                           ; preds = %parse
//   %4 = zext i1 %error_in_row1 to i8
//   store i8 %4, i8* %error_in_row
=======
//   %2 = call i1 @WriteSlot({ i8, %"struct.impala::StringValue" }* %tuple_ptr,
//                           i8* %data, i32 %len)
//   %slot_parse_error = xor i1 %2, true
//   %error_in_row2 = or i1 false, %slot_parse_error
//   %3 = zext i1 %slot_parse_error to i8
//   store i8 %3, i8* %slot_error_ptr
//   %4 = call %"class.impala::ExprContext"* @GetConjunctCtx(
//       %"class.impala::HdfsScanner"* %this, i32 0)
//   %conjunct_eval = call i16 @Eq_StringVal_StringValWrapper1(
//       %"class.impala::ExprContext"* %4, %"class.impala::TupleRow"* %tuple_row)
//   %5 = ashr i16 %conjunct_eval, 8
//   %6 = trunc i16 %5 to i8
//   %val = trunc i8 %6 to i1
//   br i1 %val, label %parse3, label %eval_fail
// 
// parse3:                                           ; preds = %parse
//   %7 = zext i1 %error_in_row2 to i8
//   store i8 %7, i8* %error_in_row
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
//   ret i1 true
// 
// eval_fail:                                        ; preds = %parse
//   ret i1 false
// }
Function* HdfsScanner::CodegenWriteCompleteTuple(
<<<<<<< HEAD
      HdfsScanNode* node, LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());
  // TODO: Timestamp is not yet supported
  for (int i = 0; i < node->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = node->materialized_slots()[i];
    if (slot_desc->type() == TYPE_TIMESTAMP) {
      LOG(WARNING) << "Could not codegen for HdfsTextScanner because timestamp "
                   << "slots are not supported.";
      return NULL;
    }
  }

  // TODO: can't codegen yet if strings need to be copied
  if (node->compact_data() && !node->tuple_desc()->string_slots().empty()) {
    return NULL;
  }

  // Codegen for eval conjuncts
  const vector<Expr*>& conjuncts = node->conjuncts();
  for (int i = 0; i < conjuncts.size(); ++i) {
    if (conjuncts[i]->codegen_fn() == NULL) return NULL;
    // TODO: handle cases with scratch buffer.
    DCHECK_EQ(conjuncts[i]->scratch_buffer_size(), 0);
=======
    HdfsScanNode* node, LlvmCodeGen* codegen, const vector<ExprContext*>& conjunct_ctxs) {
  SCOPED_TIMER(codegen->codegen_timer());
  RuntimeState* state = node->runtime_state();

  // TODO: Timestamp is not yet supported
  for (int i = 0; i < node->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = node->materialized_slots()[i];
    if (slot_desc->type().type == TYPE_TIMESTAMP) return NULL;
    if (slot_desc->type().type == TYPE_DECIMAL) return NULL;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  // Cast away const-ness.  The codegen only sets the cached typed llvm struct.
  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc());
  vector<Function*> slot_fns;
  for (int i = 0; i < node->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = node->materialized_slots()[i];
<<<<<<< HEAD
    Function* fn = TextConverter::CodegenWriteSlot(codegen, tuple_desc, slot_desc);
=======
    Function* fn = TextConverter::CodegenWriteSlot(codegen, tuple_desc, slot_desc,
        node->hdfs_table()->null_column_value().data(),
        node->hdfs_table()->null_column_value().size(), true);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    if (fn == NULL) return NULL;
    slot_fns.push_back(fn);
  }

  // Compute order to materialize slots.  BE assumes that conjuncts should
  // be evaluated in the order specified (optimization is already done by FE)
  vector<int> materialize_order;
  node->ComputeSlotMaterializationOrder(&materialize_order);

  // Get types to construct matching function signature to WriteCompleteTuple
  PointerType* uint8_ptr_type = PointerType::get(codegen->GetType(TYPE_TINYINT), 0);

  StructType* field_loc_type = reinterpret_cast<StructType*>(
      codegen->GetType(FieldLocation::LLVM_CLASS_NAME));
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  Type* tuple_opaque_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* mem_pool_type = codegen->GetType(MemPool::LLVM_CLASS_NAME);
  Type* hdfs_scanner_type = codegen->GetType(HdfsScanner::LLVM_CLASS_NAME);

  DCHECK(tuple_opaque_type != NULL);
  DCHECK(tuple_row_type != NULL);
  DCHECK(field_loc_type != NULL);
  DCHECK(hdfs_scanner_type != NULL);

  PointerType* field_loc_ptr_type = PointerType::get(field_loc_type, 0);
  PointerType* tuple_opaque_ptr_type = PointerType::get(tuple_opaque_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* mem_pool_ptr_type = PointerType::get(mem_pool_type, 0);
  PointerType* hdfs_scanner_ptr_type = PointerType::get(hdfs_scanner_type, 0);

  // Generate the typed llvm struct for the output tuple
  StructType* tuple_type = tuple_desc->GenerateLlvmStruct(codegen);
  if (tuple_type == NULL) return NULL;
  PointerType* tuple_ptr_type = PointerType::get(tuple_type, 0);

  // Initialize the function prototype.  This needs to match
  // HdfsScanner::WriteCompleteTuple's signature identically.
  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteCompleteTuple", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", hdfs_scanner_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mem_pool_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fields", field_loc_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("template", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_fields", uint8_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_in_row", uint8_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[8];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  BasicBlock* parse_block = BasicBlock::Create(context, "parse", fn);
  BasicBlock* eval_fail_block = BasicBlock::Create(context, "eval_fail", fn);

  // Extract the input args
<<<<<<< HEAD
  Value* fields_arg = args[2];
  Value* tuple_arg = builder.CreateBitCast(args[3], tuple_ptr_type, "tuple_ptr");
  Value* tuple_row_arg = builder.CreateBitCast(args[4],
      PointerType::get(codegen->ptr_type(), 0), "tuple_row_ptr");
=======
  Value* this_arg = args[0];
  Value* fields_arg = args[2];
  Value* tuple_arg = builder.CreateBitCast(args[3], tuple_ptr_type, "tuple_ptr");
  Value* tuple_row_arg = args[4];
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  Value* template_arg = builder.CreateBitCast(args[5], tuple_ptr_type, "tuple_ptr");
  Value* errors_arg = args[6];
  Value* error_in_row_arg = args[7];

  // Codegen for function body
  Value* error_in_row = codegen->false_value();
  // Initialize tuple
  if (node->num_materialized_partition_keys() == 0) {
    // No partition key slots, just zero the NULL bytes.
    for (int i = 0; i < tuple_desc->num_null_bytes(); ++i) {
      Value* null_byte = builder.CreateStructGEP(tuple_arg, i, "null_byte");
      builder.CreateStore(codegen->GetIntConstant(TYPE_TINYINT, 0), null_byte);
    }
  } else {
    // Copy template tuple.
    // TODO: only copy what's necessary from the template tuple.
    codegen->CodegenMemcpy(&builder, tuple_arg, template_arg, tuple_desc->byte_size());
  }

  // Put tuple in tuple_row
  Value* tuple_row_typed =
      builder.CreateBitCast(tuple_row_arg, PointerType::get(tuple_ptr_type, 0));
  Value* tuple_row_idxs[] = { codegen->GetIntConstant(TYPE_INT, node->tuple_idx()) };
  Value* tuple_in_row_addr = builder.CreateGEP(tuple_row_typed, tuple_row_idxs);
  builder.CreateStore(tuple_arg, tuple_in_row_addr);
  builder.CreateBr(parse_block);

<<<<<<< HEAD
  // Loop through all the conjuncts in order and materialize slots as necessary
  // to evaluate the conjuncts (e.g. conjuncts[0] will have the slots it references
=======
  // Loop through all the conjuncts in order and materialize slots as necessary to
  // evaluate the conjuncts (e.g. conjunct_ctxs[0] will have the slots it references
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // first).
  // materialized_order[slot_idx] represents the first conjunct which needs that slot.
  // Slots are only materialized if its order matches the current conjunct being
  // processed.  This guarantees that each slot is materialized once when it is first
  // needed and that at the end of the materialize loop, the conjunct has everything
  // it needs (either from this iteration or previous iterations).
  builder.SetInsertPoint(parse_block);
<<<<<<< HEAD
  LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
  Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);
  for (int conjunct_idx = 0; conjunct_idx <= conjuncts.size(); ++conjunct_idx) {
=======
  for (int conjunct_idx = 0; conjunct_idx <= conjunct_ctxs.size(); ++conjunct_idx) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    for (int slot_idx = 0; slot_idx < materialize_order.size(); ++slot_idx) {
      // If they don't match, it means either the slot has already been
      // materialized for a previous conjunct or will be materialized later for
      // another conjunct.  Either case, the slot does not need to be materialized
      // yet.
      if (materialize_order[slot_idx] != conjunct_idx) continue;

<<<<<<< HEAD
      // Materialize slots[slot_idx] to evaluate conjuncts[conjunct_idx]
=======
      // Materialize slots[slot_idx] to evaluate conjunct_ctxs[conjunct_idx]
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      // All slots[i] with materialized_order[i] < conjunct_idx have already been
      // materialized by prior iterations through the outer loop

      // Extract ptr/len from fields
      Value* data_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 0),
      };
      Value* len_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 1),
      };
      Value* error_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
      };
      Value* data_ptr = builder.CreateGEP(fields_arg, data_idxs, "data_ptr");
      Value* len_ptr = builder.CreateGEP(fields_arg, len_idxs, "len_ptr");
      Value* error_ptr = builder.CreateGEP(errors_arg, error_idxs, "slot_error_ptr");
      Value* data = builder.CreateLoad(data_ptr, "data");
      Value* len = builder.CreateLoad(len_ptr, "len");

      // Call slot parse function
      Function* slot_fn = slot_fns[slot_idx];
      Value* slot_parsed = builder.CreateCall3(slot_fn, tuple_arg, data, len);
      Value* slot_error = builder.CreateNot(slot_parsed, "slot_parse_error");
      error_in_row = builder.CreateOr(error_in_row, slot_error, "error_in_row");
      slot_error = builder.CreateZExt(slot_error, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(slot_error, error_ptr);
    }

<<<<<<< HEAD
    if (conjunct_idx == conjuncts.size()) {
=======
    if (conjunct_idx == conjunct_ctxs.size()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      // In this branch, we've just materialized slots not referenced by any conjunct.
      // This slots are the last to get materialized.  If we are in this branch, the
      // tuple passed all conjuncts and should be added to the row batch.
      Value* error_ret = builder.CreateZExt(error_in_row, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(error_ret, error_in_row_arg);
      builder.CreateRet(codegen->true_value());
    } else {
<<<<<<< HEAD
      // All slots for conjuncts[conjunct_idx] are materialized, evaluate the partial
      // tuple against that conjunct and start a new parse_block for the next conjunct
      parse_block = BasicBlock::Create(context, "parse", fn, eval_fail_block);
      Function* conjunct_fn = conjuncts[conjunct_idx]->codegen_fn();

      Value* conjunct_args[] = {
        tuple_row_arg,
        ConstantPointerNull::get(codegen->ptr_type()),
        is_null_ptr
      };
      Value* result = builder.CreateCall(conjunct_fn, conjunct_args, "conjunct_eval");

      builder.CreateCondBr(result, parse_block, eval_fail_block);
=======
      // All slots for conjunct_ctxs[conjunct_idx] are materialized, evaluate the partial
      // tuple against that conjunct and start a new parse_block for the next conjunct
      parse_block = BasicBlock::Create(context, "parse", fn, eval_fail_block);
      Function* conjunct_fn;
      Status status =
          conjunct_ctxs[conjunct_idx]->root()->GetCodegendComputeFn(state, &conjunct_fn);
      if (!status.ok()) {
        stringstream ss;
        ss << "Failed to codegen conjunct: " << status.GetErrorMsg();
        state->LogError(ss.str());
        fn->eraseFromParent();
        return NULL;
      }

      Function* get_ctx_fn =
          codegen->GetFunction(IRFunction::HDFS_SCANNER_GET_CONJUNCT_CTX);
      Value* ctx = builder.CreateCall2(
          get_ctx_fn, this_arg, codegen->GetIntConstant(TYPE_INT, conjunct_idx));

      Value* conjunct_args[] = {ctx, tuple_row_arg};
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
          codegen, &builder, TYPE_BOOLEAN, conjunct_fn, conjunct_args, "conjunct_eval");
      builder.CreateCondBr(result.GetVal(), parse_block, eval_fail_block);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      builder.SetInsertPoint(parse_block);
    }
  }

<<<<<<< HEAD
  // Block if eval failed.  
=======
  // Block if eval failed.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  builder.SetInsertPoint(eval_fail_block);
  builder.CreateRet(codegen->false_value());

  codegen->OptimizeFunctionWithExprs(fn);
  return codegen->FinalizeFunction(fn);
}

<<<<<<< HEAD
Function* HdfsScanner::CodegenWriteAlignedTuples(HdfsScanNode* node, 
=======
Function* HdfsScanner::CodegenWriteAlignedTuples(HdfsScanNode* node,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    LlvmCodeGen* codegen, Function* write_complete_tuple_fn) {
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(write_complete_tuple_fn != NULL);

  Function* write_tuples_fn =
      codegen->GetFunction(IRFunction::HDFS_SCANNER_WRITE_ALIGNED_TUPLES);
  DCHECK(write_tuples_fn != NULL);

  int replaced = 0;
  write_tuples_fn = codegen->ReplaceCallSites(write_tuples_fn, false,
      write_complete_tuple_fn, "WriteCompleteTuple", &replaced);
  DCHECK_EQ(replaced, 1) << "One call site should be replaced.";
  DCHECK(write_tuples_fn != NULL);

  return codegen->FinalizeFunction(write_tuples_fn);
}

<<<<<<< HEAD
=======
Status HdfsScanner::UpdateDecompressor(const THdfsCompression::type& compression) {
  // Check whether the file in the stream has different compression from the last one.
  if (compression != decompression_type_) {
    if (decompression_type_ != THdfsCompression::NONE) {
      // Close the previous decompressor before creating a new one.
      DCHECK(decompressor_.get() != NULL);
      decompressor_->Close();
      decompressor_.reset(NULL);
    }
    // The LZO-compression scanner is implemented in a dynamically linked library and it
    // is not created at Codec::CreateDecompressor().
    if (compression != THdfsCompression::NONE && compression != THdfsCompression::LZO) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(data_buffer_pool_.get(),
        scan_node_->tuple_desc()->string_slots().empty(), compression, &decompressor_));
    }
    decompression_type_ = compression;
  }
  return Status::OK;
}

Status HdfsScanner::UpdateDecompressor(const string& codec) {
  map<const string, const THdfsCompression::type>::const_iterator
    type = Codec::CODEC_MAP.find(codec);

  if (type == Codec::CODEC_MAP.end()) {
    stringstream ss;
    ss << Codec::UNKNOWN_CODEC_ERROR << codec;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(UpdateDecompressor(type->second));
  return Status::OK;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
bool HdfsScanner::ReportTupleParseError(FieldLocation* fields, uint8_t* errors,
    int row_idx) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    if (errors[i]) {
      const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
      ReportColumnParseError(desc, fields[i].start, fields[i].len);
      errors[i] = false;
    }
  }

  // Call into subclass to log a more accurate error message.
  if (state_->LogHasSpace()) {
    stringstream ss;
<<<<<<< HEAD
    ss << "file: " << context_->filename() << endl << "record: ";
    LogRowParseError(&ss, row_idx);
    state_->LogError(ss.str());
  }
  
  ++num_errors_in_file_;
  if (state_->abort_on_error()) {
    state_->ReportFileErrors(context_->filename(), 1);
    parse_status_ = Status(state_->ErrorLog());
=======
    ss << "file: " << stream_->filename() << endl << "record: ";
    LogRowParseError(row_idx, &ss);
    state_->LogError(ss.str());
  }

  ++num_errors_in_file_;
  if (state_->abort_on_error()) {
    state_->ReportFileErrors(stream_->filename(), 1);
    DCHECK(!parse_status_.ok());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return parse_status_.ok();
}

<<<<<<< HEAD
void HdfsScanner::LogRowParseError(stringstream* ss, int row_idx) {
=======
void HdfsScanner::LogRowParseError(int row_idx, stringstream* ss) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // This is only called for text and seq files which should override this function.
  DCHECK(false);
}

<<<<<<< HEAD
void HdfsScanner::ReportColumnParseError(const SlotDescriptor* desc, 
    const char* data, int len) {
  if (state_->LogHasSpace()) {
    stringstream ss;
    ss << "Error converting column: " 
       << desc->col_pos() - scan_node_->num_partition_keys()
       << " TO " << TypeToString(desc->type()) 
       << " (Data is: " << string(data,len) << ")";
    state_->LogError(ss.str());
  }
}
=======
void HdfsScanner::ReportColumnParseError(const SlotDescriptor* desc,
    const char* data, int len) {
  // len < 0 is used to indicate the data contains escape characters.  We don't care
  // about that here and can just output the raw string.
  if (len < 0) len = -len;

  if (state_->LogHasSpace() || state_->abort_on_error()) {
    stringstream ss;
    ss << "Error converting column: "
       << desc->col_pos() - scan_node_->num_partition_keys()
       << " TO " << desc->type()
       << " (Data is: " << string(data,len) << ")";
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    if (state_->abort_on_error() && parse_status_.ok()) parse_status_ = Status(ss.str());
  }
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
