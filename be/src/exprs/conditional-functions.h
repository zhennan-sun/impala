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


#ifndef IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H
#define IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H

#include <stdint.h>

<<<<<<< HEAD
namespace impala {

class Expr;
class TupleRow;

class ConditionalFunctions {
 public:
  static void* IfBool(Expr* e, TupleRow* row);
  static void* IfInt(Expr* e, TupleRow* row);
  static void* IfFloat(Expr* e, TupleRow* row);
  static void* IfString(Expr* e, TupleRow* row);
  static void* IfTimestamp(Expr* e, TupleRow* row);
  static void* CoalesceBool(Expr* e, TupleRow* row);
  static void* CoalesceInt(Expr* e, TupleRow* row);
  static void* CoalesceFloat(Expr* e, TupleRow* row);
  static void* CoalesceString(Expr* e, TupleRow* row);
  static void* CoalesceTimestamp(Expr* e, TupleRow* row);
  // Compute function of case expr if its has_case_expr_ is false.
  static void* NoCaseComputeFn(Expr* e, TupleRow* row);
=======
#include "exprs/expr.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class TupleRow;

// Conditional functions that can be expressed as UDFs
class ConditionalFunctions {
 public:
  static TinyIntVal NullIfZero(FunctionContext* context, const TinyIntVal& val);
  static SmallIntVal NullIfZero(FunctionContext* context, const SmallIntVal& val);
  static IntVal NullIfZero(FunctionContext* context, const IntVal& val);
  static BigIntVal NullIfZero(FunctionContext* context, const BigIntVal& val);
  static FloatVal NullIfZero(FunctionContext* context, const FloatVal& val);
  static DoubleVal NullIfZero(FunctionContext* context, const DoubleVal& val);
  static DecimalVal NullIfZero(FunctionContext* context, const DecimalVal& val);

  static TinyIntVal ZeroIfNull(FunctionContext* context, const TinyIntVal& val);
  static SmallIntVal ZeroIfNull(FunctionContext* context, const SmallIntVal& val);
  static IntVal ZeroIfNull(FunctionContext* context, const IntVal& val);
  static BigIntVal ZeroIfNull(FunctionContext* context, const BigIntVal& val);
  static FloatVal ZeroIfNull(FunctionContext* context, const FloatVal& val);
  static DoubleVal ZeroIfNull(FunctionContext* context, const DoubleVal& val);
  static DecimalVal ZeroIfNull(FunctionContext* context, const DecimalVal& val);
};

// The following conditional functions require separate Expr classes to take advantage of
// short circuiting

class IsNullExpr : public Expr {
 public:
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow* row);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow* row);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow* row);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow* row);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow* row);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow* row);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow* row);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow* row);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow* row);

  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);
  virtual std::string DebugString() const { return Expr::DebugString("IsNullExpr"); }

 protected:
  friend class Expr;
  IsNullExpr(const TExprNode& node) : Expr(node) { }
};

class NullIfExpr : public Expr {
 public:
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow* row);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow* row);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow* row);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow* row);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow* row);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow* row);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow* row);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow* row);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow* row);

  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);
  virtual std::string DebugString() const { return Expr::DebugString("NullIfExpr"); }

 protected:
  friend class Expr;
  NullIfExpr(const TExprNode& node) : Expr(node) { }
};

class IfExpr : public Expr {
 public:
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow* row);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow* row);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow* row);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow* row);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow* row);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow* row);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow* row);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow* row);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow* row);

  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);
  virtual std::string DebugString() const { return Expr::DebugString("IfExpr"); }

 protected:
  friend class Expr;
  IfExpr(const TExprNode& node) : Expr(node) { }
};

class CoalesceExpr : public Expr {
 public:
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow* row);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow* row);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow* row);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow* row);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow* row);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow* row);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow* row);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow* row);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow* row);

  virtual std::string DebugString() const { return Expr::DebugString("CoalesceExpr"); }

 protected:
  friend class Expr;
  CoalesceExpr(const TExprNode& node) : Expr(node) { }
  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif
