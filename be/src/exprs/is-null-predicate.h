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


#ifndef IMPALA_EXPRS_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_IS_NULL_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"

namespace impala {

<<<<<<< HEAD
class TExprNode;

class IsNullPredicate: public Predicate {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);

 protected:
  friend class Expr;

  IsNullPredicate(const TExprNode& node);
  
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  const bool is_not_null_;
  static void* ComputeFn(Expr* e, TupleRow* row);
=======
class IsNullPredicate {
 public:
  template<typename T> static BooleanVal IsNull(FunctionContext* ctx, const T& val);
  template<typename T> static BooleanVal IsNotNull(FunctionContext* ctx, const T& val);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif
