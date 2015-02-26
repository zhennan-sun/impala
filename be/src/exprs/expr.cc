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

#include <sstream>

<<<<<<< HEAD
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/agg-expr.h"
#include "exprs/arithmetic-expr.h"
#include "exprs/binary-predicate.h"
#include "exprs/bool-literal.h"
#include "exprs/case-expr.h"
#include "exprs/cast-expr.h"
#include "exprs/compound-predicate.h"
#include "exprs/date-literal.h"
#include "exprs/float-literal.h"
#include "exprs/function-call.h"
#include "exprs/in-predicate.h"
#include "exprs/int-literal.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal-predicate.h"
#include "exprs/null-literal.h"
#include "exprs/opcode-registry.h"
#include "exprs/string-literal.h"
#include "exprs/timestamp-literal.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Data_types.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"
=======
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/PassManager.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <llvm/Transforms/Utils/UnrollLoop.h>
#include <llvm/Support/InstIterator.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/anyval-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/aggregate-functions.h"
#include "exprs/case-expr.h"
#include "exprs/cast-functions.h"
#include "exprs/compound-predicates.h"
#include "exprs/conditional-functions.h"
#include "exprs/decimal-functions.h"
#include "exprs/decimal-operators.h"
#include "exprs/hive-udf-call.h"
#include "exprs/in-predicate.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/math-functions.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Data_types.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

<<<<<<< HEAD
using namespace std;
using namespace impala;
=======
using namespace impala;
using namespace impala_udf;
using namespace std;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
using namespace llvm;

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

<<<<<<< HEAD
void* ExprValue::TryParse(const string& str, PrimitiveType type) {
  switch(type) {
    case TYPE_BOOLEAN:
      if (ParseString<bool>(str, &bool_val)) return &bool_val;
      break;
    case TYPE_TINYINT:
      if (ParseString<int8_t>(str, &tinyint_val)) return &tinyint_val;
      break;
    case TYPE_SMALLINT:
      if (ParseString<int16_t>(str, &smallint_val)) return &smallint_val;
      break;
    case TYPE_INT:
      if (ParseString<int32_t>(str, &int_val)) return &int_val;
      break;
    case TYPE_BIGINT:
      if (ParseString<int64_t>(str, &bigint_val)) return &bigint_val;
      break;
    case TYPE_FLOAT:
      if (ParseString<float>(str, &float_val)) return &float_val;
      break;
    case TYPE_DOUBLE:
      if (ParseString<double>(str, &double_val)) return &double_val;
      break;
    case TYPE_STRING:
      SetStringVal(str);
      return &string_val;
    default:
      DCHECK(false) << "Invalid type.";
  }
  return NULL;
}

Expr::Expr(PrimitiveType type, bool is_slotref)
    : opcode_(TExprOpcode::INVALID_OPCODE),
      is_slotref_(is_slotref),
      type_(type),
      codegen_fn_(NULL),
      scratch_buffer_size_(0),
      jitted_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : opcode_(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
      is_slotref_(is_slotref),
      type_(ThriftToType(node.type)),
      codegen_fn_(NULL),
      scratch_buffer_size_(0),
      jitted_compute_fn_(NULL) {
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *root_expr = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  RETURN_IF_ERROR(CreateTreeFromThrift(pool, texpr.nodes, NULL, &node_idx, root_expr));
  if (node_idx + 1 != texpr.nodes.size()) {
    // TODO: print thrift msg for diagnostic purposes.
    return Status(
        "Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  return Status::OK;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, PrimitiveType type, void* data) {
  DCHECK(data != NULL);
  Expr* result = NULL;

  switch (type) {
    case TYPE_BOOLEAN:
      result = new BoolLiteral(*reinterpret_cast<bool*>(data));
      break;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
      result = new IntLiteral(type, data);
      break;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      result = new FloatLiteral(type, data);
      break;
    case TYPE_STRING:
      result = new StringLiteral(*reinterpret_cast<StringValue*>(data));
      break;
    default:
      DCHECK(false) << "Invalid type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, PrimitiveType type, const string& str) {
  ExprValue val;
  Expr* result = NULL;

  switch (type) {
    case TYPE_BOOLEAN:
      if (ParseString<bool>(str, &val.bool_val))
        result = new BoolLiteral(&val.bool_val);
      break;
    case TYPE_TINYINT:
      if (ParseString<int8_t>(str, &val.tinyint_val))
        result = new IntLiteral(type, &val.tinyint_val);
      break;
    case TYPE_SMALLINT:
      if (ParseString<int16_t>(str, &val.smallint_val))
        result = new IntLiteral(type, &val.smallint_val);
      break;
    case TYPE_INT:
      if (ParseString<int32_t>(str, &val.int_val))
        result = new IntLiteral(type, &val.int_val);
      break;
    case TYPE_BIGINT:
      if (ParseString<int64_t>(str, &val.bigint_val))
        result = new IntLiteral(type, &val.bigint_val);
      break;
    case TYPE_FLOAT:
      if (ParseString<float>(str, &val.float_val))
        result = new FloatLiteral(type, &val.float_val);
      break;
    case TYPE_DOUBLE:
      if (ParseString<double>(str, &val.double_val))
        result = new FloatLiteral(type, &val.double_val);
      break;
    case TYPE_TIMESTAMP:
      if (ParseString<double>(str, &val.double_val))
        result = new TimestampLiteral(val.double_val);
      break;
    case TYPE_STRING:
      result = new StringLiteral(str);
      break;
    default:
      DCHECK(false) << "Unrecognized type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Status Expr::CreateExprTrees(ObjectPool* pool, const vector<TExpr>& texprs,
                             vector<Expr*>* exprs) {
  exprs->clear();
  for (int i = 0; i < texprs.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(CreateExprTree(pool, texprs[i], &expr));
    exprs->push_back(expr);
=======
FunctionContext* Expr::RegisterFunctionContext(ExprContext* ctx, RuntimeState* state,
                                               int varargs_buffer_size) {
  FunctionContext::TypeDesc return_type = AnyValUtil::ColumnTypeToTypeDesc(type_);
  vector<FunctionContext::TypeDesc> arg_types;
  for (int i = 0; i < children_.size(); ++i) {
    arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_));
  }
  context_index_ = ctx->Register(state, return_type, arg_types, varargs_buffer_size);
  return ctx->fn_context(context_index_);
}

Expr::Expr(const ColumnType& type, bool is_slotref)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      type_(type),
      output_scale_(-1),
      context_index_(-1),
      ir_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      type_(ColumnType(node.type)),
      output_scale_(-1),
      context_index_(-1),
      ir_compute_fn_(NULL) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  DCHECK(cache_entry_ == NULL);
}

void Expr::Close(RuntimeState* state, ExprContext* context,
                 FunctionContext::FunctionStateScope scope) {
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state, context, scope);
  }

  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    // This is the final, non-cloned context to close. Clean up the whole Expr.
    if (cache_entry_ != NULL) {
      LibCache::instance()->DecrementUseCount(cache_entry_);
      cache_entry_ = NULL;
    }
  }
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *ctx = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  Expr* e;
  Status status = CreateTreeFromThrift(pool, texpr.nodes, NULL, &node_idx, &e, ctx);
  if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
    status = Status(
        "Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct expr tree.\n" << status.GetErrorMsg() << "\n"
               << apache::thrift::ThriftDebugString(texpr);
  }
  return status;
}

Status Expr::CreateExprTrees(ObjectPool* pool, const vector<TExpr>& texprs,
                             vector<ExprContext*>* ctxs) {
  ctxs->clear();
  for (int i = 0; i < texprs.size(); ++i) {
    ExprContext* ctx;
    RETURN_IF_ERROR(CreateExprTree(pool, texprs[i], &ctx));
    ctxs->push_back(ctx);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

Status Expr::CreateTreeFromThrift(ObjectPool* pool, const vector<TExprNode>& nodes,
<<<<<<< HEAD
    Expr* parent, int* node_idx, Expr** root_expr) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
    // TODO: print thrift msg
=======
    Expr* parent, int* node_idx, Expr** root_expr, ExprContext** ctx) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return Status("Failed to reconstruct expression tree from thrift.");
  }
  int num_children = nodes[*node_idx].num_children;
  Expr* expr = NULL;
  RETURN_IF_ERROR(CreateExpr(pool, nodes[*node_idx], &expr));
<<<<<<< HEAD
  // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    *root_expr = expr;
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(pool, nodes, expr, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
      // TODO: print thrift msg
=======
  DCHECK(expr != NULL);
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    DCHECK(root_expr != NULL);
    DCHECK(ctx != NULL);
    *root_expr = expr;
    *ctx = pool->Add(new ExprContext(expr));
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(pool, nodes, expr, node_idx, NULL, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  return Status::OK;
}

Status Expr::CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr) {
  switch (texpr_node.node_type) {
<<<<<<< HEAD
    case TExprNodeType::AGG_EXPR: {
      if (!texpr_node.__isset.agg_expr) {
        return Status("Aggregation expression not set in thrift node");
      }
      *expr = pool->Add(new AggregateExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::ARITHMETIC_EXPR: {
      *expr = pool->Add(new ArithmeticExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::BINARY_PRED: {
      *expr = pool->Add(new BinaryPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::BOOL_LITERAL: {
      if (!texpr_node.__isset.bool_literal) {
        return Status("Boolean literal not set in thrift node");
      }
      *expr = pool->Add(new BoolLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::CASE_EXPR: {
=======
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
      *expr = pool->Add(new Literal(texpr_node));
      return Status::OK;
    case TExprNodeType::CASE_EXPR:
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node));
      return Status::OK;
<<<<<<< HEAD
    }
    case TExprNodeType::CAST_EXPR: {
      *expr = pool->Add(new CastExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::COMPOUND_PRED: {
      *expr = pool->Add(new CompoundPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::DATE_LITERAL: {
      if (!texpr_node.__isset.date_literal) {
        return Status("Date literal not set in thrift node");
      }
      *expr = pool->Add(new DateLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::FLOAT_LITERAL: {
      if (!texpr_node.__isset.float_literal) {
        return Status("Float literal not set in thrift node");
      }
      *expr = pool->Add(new FloatLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::FUNCTION_CALL: {
      *expr = pool->Add(new FunctionCall(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::INT_LITERAL: {
      if (!texpr_node.__isset.int_literal) {
        return Status("Int literal not set in thrift node");
      }
      *expr = pool->Add(new IntLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::IN_PRED: {
      if (!texpr_node.__isset.in_predicate) {
        return Status("In predicate not set in thrift node");
      }
      *expr = pool->Add(new InPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::IS_NULL_PRED: {
      if (!texpr_node.__isset.is_null_pred) {
        return Status("Is null predicate not set in thrift node");
      }
      *expr = pool->Add(new IsNullPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::LIKE_PRED: {
      *expr = pool->Add(new LikePredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::LITERAL_PRED: {
      if (!texpr_node.__isset.literal_pred) {
        return Status("Literal predicate not set in thrift node");
      }
      *expr = pool->Add(new LiteralPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::NULL_LITERAL: {
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::SLOT_REF: {
=======
    case TExprNodeType::COMPOUND_PRED:
      if (texpr_node.fn.name.function_name == "and") {
        *expr = pool->Add(new AndPredicate(texpr_node));
      } else if (texpr_node.fn.name.function_name == "or") {
        *expr = pool->Add(new OrPredicate(texpr_node));
      } else {
        DCHECK_EQ(texpr_node.fn.name.function_name, "not");
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      return Status::OK;
    case TExprNodeType::NULL_LITERAL:
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::SLOT_REF:
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      return Status::OK;
<<<<<<< HEAD
    }
    case TExprNodeType::STRING_LITERAL: {
      if (!texpr_node.__isset.string_literal) {
        return Status("String literal not set in thrift node");
      }
      *expr = pool->Add(new StringLiteral(texpr_node));
      return Status::OK;
    }
=======
    case TExprNodeType::TUPLE_IS_NULL_PRED:
      *expr = pool->Add(new TupleIsNullPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::FUNCTION_CALL:
      if (!texpr_node.__isset.fn) {
        return Status("Function not set in thrift node");
      }
      // Special-case functions that have their own Expr classes
      // TODO: is there a better way to do this?
      if (texpr_node.fn.name.function_name == "if") {
        *expr = pool->Add(new IfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "nullif") {
        *expr = pool->Add(new NullIfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "isnull" ||
                 texpr_node.fn.name.function_name == "ifnull" ||
                 texpr_node.fn.name.function_name == "nvl") {
        *expr = pool->Add(new IsNullExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "coalesce") {
        *expr = pool->Add(new CoalesceExpr(texpr_node));

      } else if (texpr_node.fn.binary_type == TFunctionBinaryType::HIVE) {
        *expr = pool->Add(new HiveUdfCall(texpr_node));
      } else {
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      return Status::OK;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    default:
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
}

struct MemLayoutData {
  int expr_idx;
<<<<<<< HEAD
  int byte_size;  
=======
  int byte_size;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  bool variable_length;

  // TODO: sort by type as well?  Any reason to do this?
  bool operator<(const MemLayoutData& rhs) const {
    // variable_len go at end
    if (this->variable_length && !rhs.variable_length) return false;
    if (!this->variable_length && rhs.variable_length) return true;
    return this->byte_size < rhs.byte_size;
  }
};

int Expr::ComputeResultsLayout(const vector<Expr*>& exprs, vector<int>* offsets,
    int* var_result_begin) {
  if (exprs.size() == 0) {
    *var_result_begin = -1;
    return 0;
  }

  vector<MemLayoutData> data;
  data.resize(exprs.size());
<<<<<<< HEAD
  
  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    data[i].expr_idx = i;
    if (exprs[i]->type() == TYPE_STRING) {
      data[i].byte_size = 16;
      data[i].variable_length = true;
    } else {
      data[i].byte_size = GetByteSize(exprs[i]->type());
=======

  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    data[i].expr_idx = i;
    if (exprs[i]->type().IsVarLen()) {
      data[i].byte_size = 16;
      data[i].variable_length = true;
    } else {
      data[i].byte_size = exprs[i]->type().GetByteSize();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      data[i].variable_length = false;
    }
    DCHECK_NE(data[i].byte_size, 0);
  }

  sort(data.begin(), data.end());

  // Walk the types and store in a packed aligned layout
  int max_alignment = sizeof(int64_t);
  int current_alignment = data[0].byte_size;
  int byte_offset = 0;
<<<<<<< HEAD
  
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  offsets->resize(exprs.size());
  offsets->clear();
  *var_result_begin = -1;

  for (int i = 0; i < data.size(); ++i) {
    DCHECK_GE(data[i].byte_size, current_alignment);
    // Don't align more than word (8-byte) size.  This is consistent with what compilers
    // do.
<<<<<<< HEAD
    if (data[i].byte_size != current_alignment && current_alignment != max_alignment) {
=======
    if (exprs[data[i].expr_idx]->type().type == TYPE_CHAR &&
        !exprs[data[i].expr_idx]->type().IsVarLen()) {
      // CHARs are not padded, to be consistent with complier layouts
      // aligns the next value on an 8 byte boundary
      current_alignment = (data[i].byte_size + current_alignment) % max_alignment;
    } else if (data[i].byte_size != current_alignment &&
               current_alignment != max_alignment) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      byte_offset += data[i].byte_size - current_alignment;
      current_alignment = min(data[i].byte_size, max_alignment);
    }
    (*offsets)[data[i].expr_idx] = byte_offset;
    if (data[i].variable_length && *var_result_begin == -1) {
      *var_result_begin = byte_offset;
    }
    byte_offset += data[i].byte_size;
  }

  return byte_offset;
}

<<<<<<< HEAD
void Expr::GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val) {
  void* value = GetValue(row);
  if (as_ascii) {
    RawValue::PrintValue(value, type(), &col_val->stringVal);
    col_val->__isset.stringVal = true;
    return;
  }
  if (value == NULL) return;

  StringValue* string_val = NULL;
  string tmp;
  switch (type_) {
    case TYPE_BOOLEAN:
      col_val->boolVal = *reinterpret_cast<bool*>(value);
      col_val->__isset.boolVal = true;
      break;
    case TYPE_TINYINT:
      col_val->intVal = *reinterpret_cast<int8_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_SMALLINT:
      col_val->intVal = *reinterpret_cast<int16_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_INT:
      col_val->intVal = *reinterpret_cast<int32_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_BIGINT:
      col_val->longVal = *reinterpret_cast<int64_t*>(value);
      col_val->__isset.longVal = true;
      break;
    case TYPE_FLOAT:
      col_val->doubleVal = *reinterpret_cast<float*>(value);
      col_val->__isset.doubleVal = true;
      break;
    case TYPE_DOUBLE:
      col_val->doubleVal = *reinterpret_cast<double*>(value);
      col_val->__isset.doubleVal = true;
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      col_val->stringVal.swap(tmp);
      col_val->__isset.stringVal = true;
      break;
    default:
      DCHECK(false) << "bad GetValue() type: " << TypeToString(type_);
  }
}

void Expr::PrintValue(TupleRow* row, string* str) {
  RawValue::PrintValue(GetValue(row), type(), str);
}

void Expr::PrintValue(void* value, string* str) {
  RawValue::PrintValue(value, type(), str);
}

void Expr::PrintValue(void* value, stringstream* stream) {
  RawValue::PrintValue(value, type(), stream);
}

void Expr::PrintValue(TupleRow* row, stringstream* stream) {
  RawValue::PrintValue(GetValue(row), type(), stream);
}

Status Expr::PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK(type_ != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc));
=======
int Expr::ComputeResultsLayout(const vector<ExprContext*>& ctxs, vector<int>* offsets,
    int* var_result_begin) {
  vector<Expr*> exprs;
  for (int i = 0; i < ctxs.size(); ++i) exprs.push_back(ctxs[i]->root());
  return ComputeResultsLayout(exprs, offsets, var_result_begin);
}

void Expr::Close(const vector<ExprContext*>& ctxs, RuntimeState* state) {
  for (int i = 0; i < ctxs.size(); ++i) {
    ctxs[i]->Close(state);
  }
}

Status Expr::Prepare(const vector<ExprContext*>& ctxs, RuntimeState* state,
                     const RowDescriptor& row_desc, MemTracker* tracker) {
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Prepare(state, row_desc, tracker));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
Status Expr::Prepare(Expr* root, RuntimeState* state, const RowDescriptor& row_desc,
    bool disable_codegen) {
  RETURN_IF_ERROR(root->Prepare(state, row_desc));
  LlvmCodeGen* codegen = NULL;
  // state might be NULL when called from Expr-test
  if (state != NULL) codegen = state->llvm_codegen();

  // codegen == NULL means jitting is disabled.
  if (!disable_codegen && codegen != NULL && root->IsJittable(codegen)) {
    root->CodegenExprTree(codegen);
=======
Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                     ExprContext* context) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc, context));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  PrepareChildren(state, row_desc);
  // Not all exprs have opcodes (i.e. literals, agg-exprs)
  DCHECK(opcode_ != TExprOpcode::INVALID_OPCODE);
  compute_fn_ = OpcodeRegistry::Instance()->GetFunction(opcode_);
  if (compute_fn_ == NULL) {
    stringstream out;
    out << "Expr::Prepare(): Opcode: " << opcode_ << " does not have a registry entry. ";
    return Status(out.str());
=======
Status Expr::Open(const vector<ExprContext*>& ctxs, RuntimeState* state) {
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Open(state));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
Status Expr::Prepare(const vector<Expr*>& exprs, RuntimeState* state,
                     const RowDescriptor& row_desc, bool disable_codegen) {
  for (int i = 0; i < exprs.size(); ++i) {
    RETURN_IF_ERROR(Prepare(exprs[i], state, row_desc, disable_codegen));
=======
Status Expr::Open(RuntimeState* state, ExprContext* context,
                  FunctionContext::FunctionStateScope scope) {
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Open(state, context, scope));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
bool Expr::IsCodegenAvailable(const vector<Expr*>& exprs) {
  for (int i = 0; i < exprs.size(); ++i) {
    if (exprs[i]->codegen_fn() == NULL) return false;
  }
  return true;
=======
Status Expr::Clone(const vector<ExprContext*>& ctxs, RuntimeState* state,
                   vector<ExprContext*>* new_ctxs) {
  DCHECK(new_ctxs != NULL);
  DCHECK(new_ctxs->empty());
  new_ctxs->resize(ctxs.size());
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Clone(state, &(*new_ctxs)[i]));
  }
  return Status::OK;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
<<<<<<< HEAD
  out << " type=" << TypeToString(type_);
  if (opcode_ != TExprOpcode::INVALID_OPCODE) {
    out << " opcode=" << opcode_;
  }
  out << " codegen=" << (codegen_fn_ == NULL ? "false" : "true");
=======
  out << " type=" << type_.DebugString();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  if (!children_.empty()) {
    out << " children=" << DebugString(children_);
  }
  return out.str();
}

string Expr::DebugString(const vector<Expr*>& exprs) {
  stringstream out;
  out << "[";
  for (int i = 0; i < exprs.size(); ++i) {
    out << (i == 0 ? "" : " ") << exprs[i]->DebugString();
  }
  out << "]";
  return out.str();
}

<<<<<<< HEAD
=======
string Expr::DebugString(const vector<ExprContext*>& ctxs) {
  vector<Expr*> exprs;
  for (int i = 0; i < ctxs.size(); ++i) exprs.push_back(ctxs[i]->root());
  return DebugString(exprs);
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
bool Expr::IsConstant() const {
  for (int i = 0; i < children_.size(); ++i) {
    if (!children_[i]->IsConstant()) return false;
  }
  return true;
}

int Expr::GetSlotIds(vector<SlotId>* slot_ids) const {
  int n = 0;
  for (int i = 0; i < children_.size(); ++i) {
    n += children_[i]->GetSlotIds(slot_ids);
  }
  return n;
}

<<<<<<< HEAD
Type* Expr::GetLlvmReturnType(LlvmCodeGen* codegen) const {
  if (type() == TYPE_STRING) {
    return codegen->GetPtrType(TYPE_STRING);
  } else  if (type() == TYPE_TIMESTAMP) {
    // TODO
    return NULL;
  } else {
    return codegen->GetType(type());
  }
}

Function* Expr::CreateComputeFnPrototype(LlvmCodeGen* codegen, const string& name) {
  DCHECK(codegen_fn_ == NULL);
  Type* ret_type = GetLlvmReturnType(codegen);
  Type* ptr_type = codegen->ptr_type();

  LlvmCodeGen::FnPrototype prototype(codegen, name, ret_type);
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", PointerType::get(ptr_type, 0)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("state_data", ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_null", 
        codegen->GetPtrType(TYPE_BOOLEAN)));

  Function* function = prototype.GeneratePrototype();
  DCHECK(function != NULL);
  codegen_fn_ = function;
  return function;
}

Value* Expr::GetNullReturnValue(LlvmCodeGen* codegen) {
  switch (type()) {
    case TYPE_BOOLEAN:
      return ConstantInt::get(codegen->context(), APInt(1, 0, true));
    case TYPE_TINYINT:
      return ConstantInt::get(codegen->context(), APInt(8, 0, true));
    case TYPE_SMALLINT:
      return ConstantInt::get(codegen->context(), APInt(16, 0, true));
    case TYPE_INT:
      return ConstantInt::get(codegen->context(), APInt(32, 0, true));
    case TYPE_BIGINT:
      return ConstantInt::get(codegen->context(), APInt(64, 0, true));
    case TYPE_FLOAT:
      return ConstantFP::get(codegen->context(), APFloat((float)0));
    case TYPE_DOUBLE:
      return ConstantFP::get(codegen->context(), APFloat((double)0));
    case TYPE_STRING:
      return llvm::ConstantPointerNull::get(codegen->GetPtrType(TYPE_STRING));
    default:
      // Add timestamp pointers
      DCHECK(false) << "Not yet implemented.";
=======
Function* Expr::GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen) {
  switch (type.type) {
    case TYPE_BOOLEAN:
      return codegen->GetFunction(IRFunction::EXPR_GET_BOOLEAN_VAL);
    case TYPE_TINYINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_TINYINT_VAL);
    case TYPE_SMALLINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_SMALLINT_VAL);
    case TYPE_INT:
      return codegen->GetFunction(IRFunction::EXPR_GET_INT_VAL);
    case TYPE_BIGINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_BIGINT_VAL);
    case TYPE_FLOAT:
      return codegen->GetFunction(IRFunction::EXPR_GET_FLOAT_VAL);
    case TYPE_DOUBLE:
      return codegen->GetFunction(IRFunction::EXPR_GET_DOUBLE_VAL);
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return codegen->GetFunction(IRFunction::EXPR_GET_STRING_VAL);
    case TYPE_TIMESTAMP:
      return codegen->GetFunction(IRFunction::EXPR_GET_TIMESTAMP_VAL);
    case TYPE_DECIMAL:
      return codegen->GetFunction(IRFunction::EXPR_GET_DECIMAL_VAL);
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return NULL;
  }
}

<<<<<<< HEAD
void Expr::CodegenSetIsNullArg(LlvmCodeGen* codegen, BasicBlock* block, bool val) {
  LlvmCodeGen::LlvmBuilder builder(block);
  Function::arg_iterator func_args = block->getParent()->arg_begin();
  ++func_args;
  ++func_args;
  Value* is_null_ptr = func_args;
  Value* value = val ? codegen->true_value() : codegen->false_value();
  builder.CreateStore(value, is_null_ptr);
}

Value* Expr::CodegenGetValue(LlvmCodeGen* codegen, BasicBlock* caller, 
    Value* args[3], BasicBlock* null_block, BasicBlock* not_null_block) {
  DCHECK(codegen_fn() != NULL);
  LlvmCodeGen::LlvmBuilder builder(caller);
  Value* result = builder.CreateCall3(
      codegen_fn(), args[0], args[1], args[2], "child_result");
  Value* is_null_val = builder.CreateLoad(args[2], "child_null");
  builder.CreateCondBr(is_null_val, null_block, not_null_block);
  return result;
}

Value* Expr::CodegenGetValue(LlvmCodeGen* codegen, BasicBlock* parent, 
    BasicBlock* null_block, BasicBlock* not_null_block) {
  Function::arg_iterator parent_args = parent->getParent()->arg_begin();
  Value* args[3] = { parent_args++, parent_args++, parent_args };
  return CodegenGetValue(codegen, parent, args, null_block, not_null_block);
}

// typedefs for jitted compute functions  
typedef bool (*BoolComputeFn)(TupleRow*, char* , bool*);
typedef int8_t (*TinyIntComputeFn)(TupleRow*, char*, bool*);
typedef int16_t (*SmallIntComputeFn)(TupleRow*, char*, bool*);
typedef int32_t (*IntComputeFn)(TupleRow*, char*, bool*);
typedef int64_t (*BigintComputeFn)(TupleRow*, char*, bool*);
typedef float (*FloatComputeFn)(TupleRow*, char*, bool*);
typedef double (*DoubleComputeFn)(TupleRow*, char*, bool*);
typedef StringValue* (*StringValueComputeFn)(TupleRow*, char*, bool*);

void* Expr::EvalCodegendComputeFn(Expr* expr, TupleRow* row) {
  DCHECK(expr->jitted_compute_fn_ != NULL);
  void* func = expr->jitted_compute_fn_;
  void* result = NULL;
  bool is_null = false;
  switch (expr->type()) {
    case TYPE_BOOLEAN: {
      BoolComputeFn new_func = reinterpret_cast<BoolComputeFn>(func);
      expr->result_.bool_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.bool_val);
      break;
    }
    case TYPE_TINYINT: {
      TinyIntComputeFn new_func = reinterpret_cast<TinyIntComputeFn>(func);
      expr->result_.tinyint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.tinyint_val);
      break;
    }
    case TYPE_SMALLINT: {
      SmallIntComputeFn new_func = reinterpret_cast<SmallIntComputeFn>(func);
      expr->result_.smallint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.smallint_val);
      break;
    }
    case TYPE_INT: {
      IntComputeFn new_func = reinterpret_cast<IntComputeFn>(func);
      expr->result_.int_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.int_val);
      break;
    }
    case TYPE_BIGINT: {
      BigintComputeFn new_func = reinterpret_cast<BigintComputeFn>(func);
      expr->result_.bigint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.bigint_val);
      break;
    }
    case TYPE_FLOAT: {
      FloatComputeFn new_func = reinterpret_cast<FloatComputeFn>(func);
      expr->result_.float_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.float_val);
      break;
    }
    case TYPE_DOUBLE: {
      DoubleComputeFn new_func = reinterpret_cast<DoubleComputeFn>(func);
      expr->result_.double_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.double_val);
      break;
    }
    case TYPE_STRING: {
      StringValueComputeFn new_func = reinterpret_cast<StringValueComputeFn>(func);
      result = new_func(row, NULL, &is_null);
      break;
    }
    default:
      DCHECK(false) << expr->type();
      break;
  }
  if (is_null) return NULL;
  return result;
}

void Expr::SetComputeFn(void* jitted_function, int scratch_size) {
  DCHECK_EQ(scratch_size, 0);
  jitted_compute_fn_ = jitted_function;
  compute_fn_ = Expr::EvalCodegendComputeFn;
}

bool Expr::IsJittable(LlvmCodeGen* codegen) const {
  if (type() == TYPE_TIMESTAMP) return false;
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (!children()[i]->IsJittable(codegen)) return false;
  }
  return true;
}

Function* Expr::CodegenExprTree(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());
  return this->Codegen(codegen);
=======
Function* Expr::CreateIrFunctionPrototype(LlvmCodeGen* codegen, const string& name,
                                          Value* (*args)[2]) {
  Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
  LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable(
          "context", codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("row", codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME)));
  Function* function = prototype.GeneratePrototype(NULL, args[0]);
  DCHECK(function != NULL);
  return function;
}

void Expr::InitBuiltinsDummy() {
  // Call one function from each of the classes to pull all the symbols
  // from that class in.
  // TODO: is there a better way to do this?
  AggregateFunctions::InitNull(NULL, NULL);
  CastFunctions::CastToBooleanVal(NULL, TinyIntVal::null());
  CompoundPredicate::Not(NULL, BooleanVal::null());
  ConditionalFunctions::NullIfZero(NULL, TinyIntVal::null());
  DecimalFunctions::Precision(NULL, DecimalVal::null());
  DecimalOperators::CastToDecimalVal(NULL, DecimalVal::null());
  InPredicate::In(NULL, BigIntVal::null(), 0, NULL);
  IsNullPredicate::IsNull(NULL, BooleanVal::null());
  LikePredicate::Like(NULL, StringVal::null(), StringVal::null());
  Operators::Add_IntVal_IntVal(NULL, IntVal::null(), IntVal::null());
  MathFunctions::Pi(NULL);
  StringFunctions::Length(NULL, StringVal::null());
  TimestampFunctions::Year(NULL, TimestampVal::null());
  UdfBuiltins::Pi(NULL);
  UtilityFunctions::Pid(NULL);
}

AnyVal* Expr::GetConstVal(ExprContext* context) {
  DCHECK(context->opened_);
  if (!IsConstant()) return NULL;
  if (constant_val_.get() != NULL) return constant_val_.get();

  switch (type_.type) {
    case TYPE_BOOLEAN: {
      constant_val_.reset(new BooleanVal(GetBooleanVal(context, NULL)));
      break;
    }
    case TYPE_TINYINT: {
      constant_val_.reset(new TinyIntVal(GetTinyIntVal(context, NULL)));
      break;
    }
    case TYPE_SMALLINT: {
      constant_val_.reset(new SmallIntVal(GetSmallIntVal(context, NULL)));
      break;
    }
    case TYPE_INT: {
      constant_val_.reset(new IntVal(GetIntVal(context, NULL)));
      break;
    }
    case TYPE_BIGINT: {
      constant_val_.reset(new BigIntVal(GetBigIntVal(context, NULL)));
      break;
    }
    case TYPE_FLOAT: {
      constant_val_.reset(new FloatVal(GetFloatVal(context, NULL)));
      break;
    }
    case TYPE_DOUBLE: {
      constant_val_.reset(new DoubleVal(GetDoubleVal(context, NULL)));
      break;
    }
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
      constant_val_.reset(new StringVal(GetStringVal(context, NULL)));
      break;
    }
    case TYPE_TIMESTAMP: {
      constant_val_.reset(new TimestampVal(GetTimestampVal(context, NULL)));
      break;
    }
    case TYPE_DECIMAL: {
      constant_val_.reset(new DecimalVal(GetDecimalVal(context, NULL)));
      break;
    }
    default:
      DCHECK(false) << "Type not implemented: " << type();
  }
  DCHECK(constant_val_.get() != NULL);
  return constant_val_.get();
}

Status Expr::GetCodegendComputeFnWrapper(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK;
  }
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  Function* static_getval_fn = GetStaticGetValWrapper(type(), codegen);

  // Call it passing this as the additional first argument.
  Value* args[2];
  ir_compute_fn_ = CreateIrFunctionPrototype(codegen, "CodegenComputeFnWrapper", &args);
  BasicBlock* entry_block =
      BasicBlock::Create(codegen->context(), "entry", ir_compute_fn_);
  LlvmCodeGen::LlvmBuilder builder(entry_block);
  Value* this_ptr =
      codegen->CastPtrToLlvmPtr(codegen->GetPtrType(Expr::LLVM_CLASS_NAME), this);
  Value* compute_fn_args[] = { this_ptr, args[0], args[1] };
  Value* ret = CodegenAnyVal::CreateCall(
      codegen, &builder, static_getval_fn, compute_fn_args, "ret");
  builder.CreateRet(ret);
  ir_compute_fn_ = codegen->FinalizeFunction(ir_compute_fn_);
  *fn = ir_compute_fn_;
  return Status::OK;
}

// At least one of these should always be subclassed.
BooleanVal Expr::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return BooleanVal::null();
}
TinyIntVal Expr::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return TinyIntVal::null();
}
SmallIntVal Expr::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return SmallIntVal::null();
}
IntVal Expr::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return IntVal::null();
}
BigIntVal Expr::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return BigIntVal::null();
}
FloatVal Expr::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return FloatVal::null();
}
DoubleVal Expr::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return DoubleVal::null();
}
StringVal Expr::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return StringVal::null();
}
TimestampVal Expr::GetTimestampVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return TimestampVal::null();
}
DecimalVal Expr::GetDecimalVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return DecimalVal::null();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
