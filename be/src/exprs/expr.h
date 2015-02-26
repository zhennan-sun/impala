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


<<<<<<< HEAD
=======
// --- Terminology:
//
// Compute function: The function that, given a row, performs the computation of an expr
// and produces a scalar result. This function evaluates the necessary child arguments by
// calling their compute functions, then performs whatever computation is necessary on the
// arguments (e.g. calling a UDF with the child arguments). All compute functions take
// arguments (ExprContext*, TupleRow*). The return type is a *Val (i.e. a subclass of
// AnyVal). Thus, a single expression will implement a compute function for every return
// type it supports.
//
// UDX: user-defined X. E.g., user-defined function, user-defined aggregate. Something
// that is written by an external user.
//
// Scalar function call: An expr that returns a single scalar value and can be implemented
// using the UDF interface. Note that this includes builtins, which although not being
// user-defined still use the same interface as UDFs (i.e., they are implemented as
// functions with signature "*Val (FunctionContext*, *Val, *Val...)").
//
// Aggregate function call: a UDA or builtin aggregate function.
//
// --- Expr overview:
//
// The Expr superclass defines a virtual Get*Val() compute function for each possible
// return type (GetBooleanVal(), GetStringVal(), etc). Expr subclasses implement the
// Get*Val() functions associated with their possible return types; for many Exprs this
// will be a single function. These functions are generally cross-compiled to both native
// and IR libraries. In the interpreted path, the native compute functions are run as-is.
//
// For the codegen path, Expr defines a virtual method GetCodegendComputeFn() that returns
// the Function* of the expr's compute function. Note that we do not need a separate
// GetCodegendComputeFn() for each type.
//
// Only short-circuited operators (e.g. &&, ||) and other special functions like literals
// must implement custom Get*Val() compute functions. Scalar function calls use the
// generic compute functions implemented by ScalarFnCall(). For cross-compiled compute
// functions, GetCodegendComputeFn() can use ReplaceChildCallsComputeFn(), which takes a
// cross-compiled IR Get*Val() function, pulls out any calls to the children's Get*Val()
// functions (which we identify via the Get*Val() static wrappers), and replaces them with
// the codegen'd version of that function. This allows us to write a single function for
// both the interpreted and codegen paths.
//
// --- Expr users (e.g. exec nodes):
//
// A typical usage pattern will look something like:
// 1. Expr::CreateExprTrees()
// 2. Expr::Prepare()
// 3. Expr::Open()
// 4. Expr::Clone() [for multi-threaded execution]
// 5. Evaluate exprs via Get*Val() calls
// 6. Expr::Close() [called once per ExprContext, including clones]
//
// Expr users should use the static Get*Val() wrapper functions to evaluate exprs,
// cross-compile the resulting function, and use ReplaceGetValCalls() to create the
// codegen'd function. See the comments on these functions for more details. This is a
// similar pattern to that used by the cross-compiled compute functions.
//
// TODO:
// - Fix codegen compile time
// - Fix perf regressions via extra optimization passes + patching LLVM

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

#include "common/status.h"
<<<<<<< HEAD
#include "gen-cpp/Opcodes_types.h"
#include "runtime/descriptors.h"
=======
#include "impala-ir/impala-ir-functions.h"
#include "runtime/descriptors.h"
#include "runtime/decimal-value.h"
#include "runtime/lib-cache.h"
#include "runtime/raw-value.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
<<<<<<< HEAD
=======
#include "udf/udf.h"

using namespace impala_udf;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

namespace llvm {
  class BasicBlock;
  class Function;
  class Type;
  class Value;
};

namespace impala {

class Expr;
<<<<<<< HEAD
=======
class IsNullExpr;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class LlvmCodeGen;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;

<<<<<<< HEAD
// The materialized value returned by Expr::GetValue().
// Some exprs may set multiple fields of this value at once
// for maintaining state across evaluations.
// For example, the rand() math function uses double_val as its return value,
// and int_val as the state for the random number generator.
struct ExprValue {
  bool bool_val;
  int8_t tinyint_val;
  int16_t smallint_val;
  int32_t int_val;
  int64_t bigint_val;
  float float_val;
  double double_val;
  std::string string_data;
  StringValue string_val;
  TimestampValue timestamp_val;

  ExprValue()
    : bool_val(false),
      tinyint_val(0),
      smallint_val(0),
      int_val(0),
      bigint_val(0),
      float_val(0.0),
      double_val(0.0),
      string_data(),
      string_val(NULL, 0),
      timestamp_val() {
  }

  ExprValue(bool v): bool_val(v) {}
  ExprValue(int8_t v): tinyint_val(v) {}
  ExprValue(int16_t v): smallint_val(v) {}
  ExprValue(int32_t v): int_val(v) {}
  ExprValue(int64_t v): bigint_val(v) {}
  ExprValue(float v): float_val(v) {}
  ExprValue(double v): double_val(v) {}
  ExprValue(int64_t t, int64_t n) : timestamp_val(t, n) {}

  // c'tor for string values
  ExprValue(const std::string& str)
    : string_data(str),
      string_val(const_cast<char*>(string_data.data()), string_data.size()) {
  }

  // Update this ExprValue by parsing the string and return a pointer to the result.
  // NULL will be returned if the string and type are not compatible.
  void* TryParse(const std::string& string, PrimitiveType type);

  // Set string value to copy of str
  void SetStringVal(const StringValue& str) {
    string_data = std::string(str.ptr, str.len);
    SyncStringVal();
  }

  void SetStringVal(const std::string& str) {
    string_data = str;
    SyncStringVal();
  }

  // Updates string_val ptr / len pair to reflect any changes in
  // string_data. If not called after mutating string_data,
  // string_val->ptr may point at garbage.
  void SyncStringVal() {
    string_val.ptr = const_cast<char*>(string_data.data());
    string_val.len = string_data.size();
  }

  // Sets the value for type to '0' and returns a pointer to the data
  void* SetToZero(PrimitiveType type) {
    switch (type) {
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = 0;
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = 0;
        return &smallint_val;
      case TYPE_INT:
        int_val = 0;
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = 0;
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = 0;
        return &float_val;
      case TYPE_DOUBLE:
        double_val = 0;
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }
  
  // Sets the value for type to min and returns a pointer to the data
  void* SetToMin(PrimitiveType type) {
    switch (type) {
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::min();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::min();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::min();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::min();
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = std::numeric_limits<float>::min();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = std::numeric_limits<double>::min();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }
  
  // Sets the value for type to max and returns a pointer to the data
  void* SetToMax(PrimitiveType type) {
    switch (type) {
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::max();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::max();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::max();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::max();
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = std::numeric_limits<float>::max();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = std::numeric_limits<double>::max();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }
};

// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  // typedef for compute functions.
  typedef void* (*ComputeFn)(Expr*, TupleRow*);

  // Evaluate expr and return pointer to result. The result is
  // valid as long as 'row' doesn't change.
  // TODO: stop having the result cached in this Expr object 
  void* GetValue(TupleRow* row);

  // Convenience function: extract value into col_val and sets the
  // appropriate __isset flag.
  // If the value is NULL and as_ascii is false, nothing is set.
  // If 'as_ascii' is true, writes the value in ascii into stringVal
  // (nulls turn into "NULL");
  // if it is false, the specific field in col_val that receives the value is
  // based on the type of the expr:
  // TYPE_BOOLEAN: boolVal
  // TYPE_TINYINT/SMALLINT/INT: intVal
  // TYPE_BIGINT: longVal
  // TYPE_FLOAT/DOUBLE: doubleVal
  // TYPE_STRING: stringVal
  void GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val);

  // Convenience functions: print value into 'str' or 'stream'.
  // NULL turns into "NULL".
  void PrintValue(TupleRow* row, std::string* str);
  void PrintValue(void* value, std::string* str);
  void PrintValue(TupleRow* value, std::stringstream* stream);
  void PrintValue(void* value, std::stringstream* stream);
=======
// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  virtual ~Expr();

  // Virtual compute functions for each *Val type. Each Expr subclass should implement the
  // functions for the return type(s) it supports. For example, a boolean function will
  // only implement GetBooleanVal(). Some Exprs, like Literal, have many possible return
  // types and will implement multiple Get*Val() functions.
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow*);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow*);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow*);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow*);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow*);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow*);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow*);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow*);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow*);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow*);

  // Get the number of digits after the decimal that should be displayed for this
  // value. Returns -1 if no scale has been specified (currently the scale is only set for
  // doubles set by RoundUpTo). GetValue() must have already been called.
  // TODO: is this still necessary?
  int output_scale() const { return output_scale_; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  void AddChild(Expr* expr) { children_.push_back(expr); }
  Expr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

<<<<<<< HEAD
  PrimitiveType type() const { return type_; }
  const std::vector<Expr*>& children() const { return children_; }

  TExprOpcode::type op() const { return opcode_; }

  // Returns true if expr doesn't contain slotrefs, ie, can be evaluated
  // with GetValue(NULL). The default implementation returns true if all of
  // the children are constant.
  virtual bool IsConstant() const;

  // Returns the slots that are referenced by this expr tree in 'slot_ids'.
  // Returns the number of slots added to the vector 
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  // Create expression tree from the list of nodes contained in texpr
  // within 'pool'. Returns root of expression tree in 'root_expr'.
  // Returns OK if successful, otherwise an error.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr);

  // Creates vector of exprs from given vector of TExprs within 'pool'.
  // Returns an error if any of the individual conversions caused an error,
  // otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                std::vector<Expr*>* exprs);

  // Prepare expr tree for evaluation, setting the compute_fn_ for each node
  // in the tree. If codegen is enabled, this function will also codegen each node
  // in the expr tree.
  // disable_codegen can be set for a particular Prepare() call to disable codegen for
  // a specific expr tree.
  static Status Prepare(Expr* root, RuntimeState* state, const RowDescriptor& row_desc,
      bool disable_codegen = false);

  // Prepare all exprs.
  static Status Prepare(const std::vector<Expr*>& exprs, RuntimeState* state,
                        const RowDescriptor& row_desc, bool disable_codegen = false);

  // Create a new literal expr of 'type' with initial 'data'.
  // data should match the PrimitiveType (i.e. type == TYPE_INT, data is a int*)
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, PrimitiveType type, void* data);
=======
  const ColumnType& type() const { return type_; }
  bool is_slotref() const { return is_slotref_; }

  const std::vector<Expr*>& children() const { return children_; }

  // Returns true if GetValue(NULL) can be called on this expr and always returns the same
  // result (e.g., exprs that don't contain slotrefs). The default implementation returns
  // true if all children are constant.
  virtual bool IsConstant() const;

  // Returns the slots that are referenced by this expr tree in 'slot_ids'.
  // Returns the number of slots added to the vector
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  // Create expression tree from the list of nodes contained in texpr within 'pool'.
  // Returns the root of expression tree in 'expr' and the corresponding ExprContext in
  // 'ctx'.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx);

  // Creates vector of ExprContexts containing exprs from the given vector of
  // TExprs within 'pool'.  Returns an error if any of the individual conversions caused
  // an error, otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
      std::vector<ExprContext*>* ctxs);

  // Convenience function for preparing multiple expr trees.
  // Allocations from 'ctxs' will be counted against 'tracker'.
  static Status Prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
                        const RowDescriptor& row_desc, MemTracker* tracker);

  // Convenience function for opening multiple expr trees.
  static Status Open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  // Clones each ExprContext for multiple expr trees. 'new_ctxs' should be an
  // empty vector, and a clone of each context in 'ctxs' will be added to it.
  // The new ExprContexts are created in state->obj_pool().
  static Status Clone(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
                      std::vector<ExprContext*>* new_ctxs);

  // Convenience function for closing multiple expr trees.
  static void Close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  // Create a new literal expr of 'type' with initial 'data'.
  // data should match the ColumnType (i.e. type == TYPE_INT, data is a int*)
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type, void* data);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Create a new literal expr of 'type' by parsing the string.
  // NULL will be returned if the string and type are not compatible.
  // The new Expr will be allocated from the pool.
<<<<<<< HEAD
  static Expr* CreateLiteral(ObjectPool* pool, PrimitiveType type, const std::string&);

  // Computes a memory efficient layout for storing the results of evaluating 'exprs'
  // Returns the number of bytes necessary to store all the results and offsets 
=======
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type,
      const std::string&);

  // Computes a memory efficient layout for storing the results of evaluating 'exprs'
  // Returns the number of bytes necessary to store all the results and offsets
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // where the result for each expr should be stored.
  // Variable length types are guaranteed to be at the end and 'var_result_begin'
  // will be set the beginning byte offset where variable length results begin.
  // 'var_result_begin' will be set to -1 if there are no variable len types.
<<<<<<< HEAD
  static int ComputeResultsLayout(const std::vector<Expr*>& exprs, 
      std::vector<int>* offsets, int* var_result_begin);

  // Returns if codegen on each of the exprs is available.
  static bool IsCodegenAvailable(const std::vector<Expr*>& exprs);

  // Codegen the expr tree rooted at this node.  This does a post order traversal
  // of the expr tree and codegen's each node.  Returns NULL if the subtree cannot
  // be codegen'd.
  // Subclasses should override this function if it supports jitting.
  // This function needs to set scratch_buffer_size_.
  // All expr codegen'd functions have this signature:
  // <expr ret type> ComputeFn(int8_t** tuple_row, int8_t* scratch_buffer, bool* is_null) 
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen) {
    return NULL;
  }

  // Returns whether the subtree at this node is jittable.  This is temporary
  // until more expr types are supported.
  virtual bool IsJittable(LlvmCodeGen*) const;

  // Returns codegen function for the expr tree rooted at this expr.
  llvm::Function* codegen_fn() { return codegen_fn_; }

  // Returns the scratch buffer size needed to call codegen_fn
  int scratch_buffer_size() { return scratch_buffer_size_; }

  // Utility function to codegen a call to this expr (tree)'s codegen'd compute function.
  // This function will codegen a call instruction, a branch to check for null and a
  // jump to either the null or non-null block.
  // Returns the result of the compute function (only valid in non-null branch).
  // - caller: the code block from the calling function.  A call instruction will be
  //   added to the end of the block.
  // - args: args to call this compute function with
  // - null_block: block in caller function to jump to if the child is null
  // - not_null_block: block in caller function to jump to if the child is not null
  llvm::Value* CodegenGetValue(LlvmCodeGen* codegen, llvm::BasicBlock* caller,
      llvm::Value** args, llvm::BasicBlock* null_block, llvm::BasicBlock* not_null_block);

  // This is a wrapper around CodegenGetValue() that is used by a parent Expr node to
  // call a child.  The parent args are used to call the child function.
  llvm::Value* CodegenGetValue(LlvmCodeGen* codegen, llvm::BasicBlock* parent,
      llvm::BasicBlock* null_block, llvm::BasicBlock* not_null_block);

  // Returns the llvm return type for this expr
  llvm::Type* GetLlvmReturnType(LlvmCodeGen* codegen) const;

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);
  
  static const char* LLVM_CLASS_NAME;

 protected:
  friend class ComputeFunctions;
=======
  static int ComputeResultsLayout(const std::vector<Expr*>& exprs,
      std::vector<int>* offsets, int* var_result_begin);
  static int ComputeResultsLayout(const std::vector<ExprContext*>& ctxs,
      std::vector<int>* offsets, int* var_result_begin);

  // Returns an llvm::Function* with signature:
  // <subclass of AnyVal> ComputeFn(ExprContext* context, TupleRow* row)
  //
  // The function should evaluate this expr over 'row' and return the result as the
  // appropriate type of AnyVal.
  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) = 0;

  // If this expr is constant, evaluates the expr with no input row argument and returns
  // the output. Returns NULL if the argument is not constant. The returned AnyVal* is
  // owned by this expr. This should only be called after Open() has been called on this
  // expr.
  virtual AnyVal* GetConstVal(ExprContext* context);

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);
  static std::string DebugString(const std::vector<ExprContext*>& ctxs);

  // The builtin functions are not called from anywhere in the code and the
  // symbols are therefore not included in the binary. We call these functions
  // by using dlsym. The compiler must think this function is callable to
  // not strip these symbols.
  static void InitBuiltinsDummy();

  static const char* LLVM_CLASS_NAME;

 protected:
  friend class AggFnEvaluator;
  friend class CastExpr;
  friend class ComputeFunctions;
  friend class DecimalFunctions;
  friend class DecimalLliteral;
  friend class DecimalOperators;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  friend class MathFunctions;
  friend class StringFunctions;
  friend class TimestampFunctions;
  friend class ConditionalFunctions;
  friend class UtilityFunctions;
  friend class CaseExpr;
  friend class InPredicate;
  friend class FunctionCall;
<<<<<<< HEAD

  Expr(PrimitiveType type, bool is_slotref = false);
  Expr(const TExprNode& node, bool is_slotref = false);

  // Prepare should be invoked recurisvely on the expr tree.
  // Return OK if successful, otherwise return error status.
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);

  // Helper function that just calls prepare on all the children
  // Does not do anything on the this expr.
  // Return OK if successful, otherwise return error status.
  Status PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc);

  // function to evaluate expr; typically set in Prepare()
  ComputeFn compute_fn_;

  // function opcode
  TExprOpcode::type opcode_;
=======
  friend class ScalarFnCall;

  Expr(const ColumnType& type, bool is_slotref = false);
  Expr(const TExprNode& node, bool is_slotref = false);

  // Initializes this expr instance for execution. This does not include initializing
  // state in the ExprContext; 'context' should only be used to register a FunctionContext
  // via RegisterFunctionContext(). Any IR functions must be generated here.
  //
  // Subclasses overriding this function should call Expr::Prepare() to recursively call
  // Prepare() on the expr tree.
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                         ExprContext* context);

  // Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
  // thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
  // thread-local state should be initialized.
  //
  // Subclasses overriding this function should call Expr::Open() to recursively call
  // Open() on the expr tree.
  virtual Status Open(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  // Subclasses overriding this function should call Expr::Close().
  //
  // If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
  // down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
  // down.
  virtual void Close(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  // Cache entry for the library implementing this function.
  LibCache::LibCacheEntry* cache_entry_;

  // Function description.
  TFunction fn_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;
  // analysis is done, types are fixed at this point
<<<<<<< HEAD
  const PrimitiveType type_;
  std::vector<Expr*> children_;
  ExprValue result_;

  // Codegened IR function.  Will be NULL if this expr was not codegen'd.
  llvm::Function* codegen_fn_;

  // Size of scratch buffer necessary to call codegen'd compute function.
  // TODO: not implemented, always 0
  int scratch_buffer_size_;

  // Create a compute function prototype.
  // The signature is:
  // <expr ret type> ComputeFn(TupleRow* row, char* state_data, bool* is_null)
  llvm::Function* CreateComputeFnPrototype(LlvmCodeGen* codegen, const std::string& name);

  // Create dummy ret value for NULL result for this expr's return type
  llvm::Value* GetNullReturnValue(LlvmCodeGen* codegen);

  // Codegen IR to set the out is_null return arg to 'val'
  void CodegenSetIsNullArg(LlvmCodeGen* codegen, llvm::BasicBlock* block, bool val);

  // Codegen call to child compute function, passing the arguments from this
  // compute function.
  // Checks NULL return from child and will conditionally branch to the
  // null/not_null block of the parent function
  // - parent: the calling function.  A call instruction will be added to this function.
  // - child: the child function to call
  // - null_block: block in parent function to jump to if the child is null
  // - not_null_block: block in parent function to jump to if the child is not null
  llvm::Value* CodegenCallFn(LlvmCodeGen* codegen, llvm::Function* parent,
      llvm::Function* child, llvm::BasicBlock* null_block,
      llvm::BasicBlock* not_null_block);

 private:
  friend class ExprTest;
  friend class QueryJitter;
=======
  const ColumnType type_;
  std::vector<Expr*> children_;
  int output_scale_;

  // Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
  // Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
  // doesn't call RegisterFunctionContext().
  int context_index_;

  // Cached codegened compute function. Exprs should set this in GetCodegendComputeFn().
  llvm::Function* ir_compute_fn_;

  // If this expr is constant, this will store and cache the value generated by
  // GetConstVal().
  boost::scoped_ptr<AnyVal> constant_val_;

  // Helper function that calls ctx->Register(), sets context_index_, and returns the
  // registered FunctionContext.
  FunctionContext* RegisterFunctionContext(
      ExprContext* ctx, RuntimeState* state, int varargs_buffer_size = 0);

  // Helper function to create an empty Function* with the appropriate signature to be
  // returned by GetCodegendComputeFn(). 'name' is the name of the returned Function*. The
  // arguments to the function are returned in 'args'.
  llvm::Function* CreateIrFunctionPrototype(LlvmCodeGen* codegen, const std::string& name,
                                            llvm::Value* (*args)[2]);

  // Generates an IR compute function that calls the appropriate interpreted Get*Val()
  // compute function.
  //
  // This is useful for builtins that can't be implemented with the UDF interface
  // (e.g. functions that need short-circuiting) and that don't have custom codegen
  // functions that use the IRBuilder. It doesn't provide any performance benefit over the
  // interpreted path.
  // TODO: this should be replaced with fancier xcompiling infrastructure
  Status GetCodegendComputeFnWrapper(RuntimeState* state, llvm::Function** fn);

  // Returns the IR version of the static Get*Val() wrapper function corresponding to
  // 'type'. This is used for calling interpreted Get*Val() functions from codegen'd
  // functions (e.g. in ScalarFnCall() when codegen is disabled).
  llvm::Function* GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen);

  // Simple debug string that provides no expr subclass-specific information
  std::string DebugString(const std::string& expr_name) const {
    std::stringstream out;
    out << expr_name << "(" << Expr::DebugString() << ")";
    return out.str();
  }

 private:
  friend class ExprContext;
  friend class ExprTest;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Create a new Expr based on texpr_node.node_type within 'pool'.
  static Status CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr);

  // Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
  // parameters
  //   nodes: vector of thrift expression nodes to be translated
  //   parent: parent of node at node_idx (or NULL for node_idx == 0)
  //   node_idx:
  //     in: root of TExprNode tree
  //     out: next node in 'nodes' that isn't part of tree
  //   root_expr: out: root of constructed expr tree
<<<<<<< HEAD
=======
  //   ctx: out: context of constructed expr tree
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // return
  //   status.ok() if successful
  //   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(ObjectPool* pool,
      const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
<<<<<<< HEAD
      Expr** root_expr);

  // Update the compute function with the jitted function.
  void SetComputeFn(void* jitted_function, int scratch_size);

  // Jit compile expr tree.  Returns a function pointer to the jitted function.
  // scratch_size is an out parameter for the required size of the scratch buffer
  // to call the jitted function.
  // Returns NULL if the function is not jittable.
  llvm::Function* CodegenExprTree(LlvmCodeGen* codegen);

  // Compute function wrapper for jitted Expr trees.  This is temporary until
  // we are generating loops to evaluate batches of tuples.
  // This is a shim to convert the old ComputeFn signature to the code-
  // generated signature.  It will call the underlying jitted function and
  // stuff the result back in expr->result_.
  static void* EvalCodegendComputeFn(Expr* expr, TupleRow* row);

  // This is a function pointer to the compute function.  The return type
  // for jitted functions depends on the Expr so we need to store it as
  // a void* and then cast it on invocation.
  void* jitted_compute_fn_;
};

// Reference to a single slot of a tuple.
// We inline this here in order for Expr::GetValue() to be able
// to reference SlotRef::ComputeFn() directly.
// Splitting it up into separate .h files would require circular #includes.
class SlotRef : public Expr {
 public:
  SlotRef(const TExprNode& node);
  SlotRef(const SlotDescriptor* desc);

  // Used for testing.  GetValue will return tuple + offset interpreted as 'type'
  SlotRef(PrimitiveType type, int offset);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  static void* ComputeFn(Expr* expr, TupleRow* row);
  virtual std::string DebugString() const;
  virtual bool IsConstant() const { return false; }
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  virtual llvm::Function* Codegen(LlvmCodeGen* codegen);

 protected:
  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
  bool tuple_is_nullable_; // true if the tuple is nullable.
};

inline void* SlotRef::ComputeFn(Expr* expr, TupleRow* row) {
  SlotRef* ref = static_cast<SlotRef*>(expr);
  Tuple* t = row->GetTuple(ref->tuple_idx_);
  if (t == NULL || t->IsNull(ref->null_indicator_offset_)) return NULL;
  return t->GetSlot(ref->slot_offset_);
}

inline void* Expr::GetValue(TupleRow* row) {
  DCHECK(type_ != INVALID_TYPE);
  if (is_slotref_) {
    return SlotRef::ComputeFn(this, row);
  } else {
    return compute_fn_(this, row);
  }
}

=======
      Expr** root_expr, ExprContext** ctx);

  // Static wrappers around the virtual Get*Val() functions. Calls the appropriate
  // Get*Val() function on expr, passing it the context and row arguments.
  //
  // These are used to call Get*Val() functions from generated functions, since I don't
  // know how to call virtual functions directly. GetStaticGetValWrapper() returns the IR
  // function of the appropriate wrapper function.
  static BooleanVal GetBooleanVal(Expr* expr, ExprContext* context, TupleRow* row);
  static TinyIntVal GetTinyIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static SmallIntVal GetSmallIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static IntVal GetIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static BigIntVal GetBigIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static FloatVal GetFloatVal(Expr* expr, ExprContext* context, TupleRow* row);
  static DoubleVal GetDoubleVal(Expr* expr, ExprContext* context, TupleRow* row);
  static StringVal GetStringVal(Expr* expr, ExprContext* context, TupleRow* row);
  static TimestampVal GetTimestampVal(Expr* expr, ExprContext* context, TupleRow* row);
  static DecimalVal GetDecimalVal(Expr* expr, ExprContext* context, TupleRow* row);
};

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

#endif
