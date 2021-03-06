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

#include "exprs/math-functions.h"

#include <iomanip>
#include <sstream>
#include <math.h>

<<<<<<< HEAD
#include "exprs/expr.h"
#include "runtime/tuple-row.h"
=======
#include "exprs/anyval-util.h"
#include "exprs/expr.h"
#include "exprs/operators.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "util/string-parser.h"

using namespace std;

<<<<<<< HEAD
namespace impala { 

const char* MathFunctions::ALPHANUMERIC_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

void* MathFunctions::Pi(Expr* e, TupleRow* row) {
  e->result_.double_val = M_PI;
  return &e->result_.double_val;
}

void* MathFunctions::E(Expr* e, TupleRow* row) {
  e->result_.double_val = M_E;
  return &e->result_.double_val;
}

void* MathFunctions::Abs(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = fabs(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Sign(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.float_val = (*d < 0) ? -1.0 : 1.0;
  return &e->result_.float_val;
}

void* MathFunctions::Sin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = sin(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Asin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = asin(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Cos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = cos(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Acos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = acos(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Tan(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = tan(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Atan(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = atan(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Radians(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = *d * M_PI / 180.0;
  return &e->result_.double_val;
}

void* MathFunctions::Degrees(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = *d * 180 / M_PI;
  return &e->result_.double_val;
}

void* MathFunctions::Ceil(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = ceil(*d);
  return &e->result_.bigint_val;
}

void* MathFunctions::Floor(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = floor(*d);
  return &e->result_.bigint_val;
}

void* MathFunctions::Round(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = static_cast<int64_t>(*d + ((*d < 0) ? -0.5 : 0.5));
  return &e->result_.bigint_val;
}

void* MathFunctions::RoundUpTo(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  int32_t* precision = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  if (d == NULL || precision == NULL) return NULL;
  e->result_.double_val = floor(*d * pow(10.0, *precision) + 0.5) / pow(10.0, *precision);
  return &e->result_.double_val;
}

void* MathFunctions::Exp(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = exp(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Ln(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Log10(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log10(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Log2(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log(*d) / log (2.0);
  return &e->result_.double_val;
}

void* MathFunctions::Log(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* base = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* val = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (val == NULL || base == NULL) return NULL;
  e->result_.double_val = log(*val) / log(*base);
  return &e->result_.double_val;
}

void* MathFunctions::Pow(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* base = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* exp = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (exp == NULL || base == NULL) return NULL;
  e->result_.double_val = pow(*base, *exp);
  return &e->result_.double_val;
}

void* MathFunctions::Sqrt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = sqrt(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Rand(Expr* e, TupleRow* row) {
  // Use e->result_.int_val as state for the random number generator.
  // Cast is necessary, otherwise rand_r will complain.
  e->result_.int_val = rand_r(reinterpret_cast<uint32_t*>(&e->result_.int_val));
  // Normalize to [0,1].
  e->result_.double_val =
      static_cast<double>(e->result_.int_val) / static_cast<double>(RAND_MAX);
  return &e->result_.double_val;
}

void* MathFunctions::RandSeed(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int32_t* seed = reinterpret_cast<int32_t*>(e->children()[0]->GetValue(row));
  if (seed == NULL) return NULL;
  // Use e->result_.bool_val to indicate whether initial seed has been set.
  if (!e->result_.bool_val) {
    // Set user-defined seed upon this first call.
    e->result_.int_val = *seed;
    e->result_.bool_val = true;
  }
  return Rand(e, row);
}

void* MathFunctions::Bin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  // Cast to an unsigned integer because it is compiler dependent
  // whether the sign bit will be shifted like a regular bit.
  // (logical vs. arithmetic shift for signed numbers)
  uint64_t* num = reinterpret_cast<uint64_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  uint64_t n = *num;
=======
namespace impala {

const char* MathFunctions::ALPHANUMERIC_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

DoubleVal MathFunctions::Pi(FunctionContext* ctx) {
  return DoubleVal(M_PI);
}

DoubleVal MathFunctions::E(FunctionContext* ctx) {
  return DoubleVal(M_E);
}

// Generates a UDF that always calls FN() on the input val and returns it.
#define ONE_ARG_MATH_FN(NAME, RET_TYPE, INPUT_TYPE, FN) \
  RET_TYPE MathFunctions::NAME(FunctionContext* ctx, const INPUT_TYPE& v) { \
    if (v.is_null) return RET_TYPE::null(); \
    return RET_TYPE(FN(v.val)); \
  }

ONE_ARG_MATH_FN(Abs, BigIntVal, BigIntVal, llabs);
ONE_ARG_MATH_FN(Abs, DoubleVal, DoubleVal, fabs);
ONE_ARG_MATH_FN(Abs, FloatVal, FloatVal, fabs);
ONE_ARG_MATH_FN(Abs, IntVal, IntVal, abs);
ONE_ARG_MATH_FN(Abs, SmallIntVal, SmallIntVal, abs);
ONE_ARG_MATH_FN(Abs, TinyIntVal, TinyIntVal, abs);
ONE_ARG_MATH_FN(Sin, DoubleVal, DoubleVal, sin);
ONE_ARG_MATH_FN(Asin, DoubleVal, DoubleVal, asin);
ONE_ARG_MATH_FN(Cos, DoubleVal, DoubleVal, cos);
ONE_ARG_MATH_FN(Acos, DoubleVal, DoubleVal, acos);
ONE_ARG_MATH_FN(Tan, DoubleVal, DoubleVal, tan);
ONE_ARG_MATH_FN(Atan, DoubleVal, DoubleVal, atan);
ONE_ARG_MATH_FN(Sqrt, DoubleVal, DoubleVal, sqrt);
ONE_ARG_MATH_FN(Ceil, BigIntVal, DoubleVal, ceil);
ONE_ARG_MATH_FN(Floor, BigIntVal, DoubleVal, floor);
ONE_ARG_MATH_FN(Ln, DoubleVal, DoubleVal, log);
ONE_ARG_MATH_FN(Log10, DoubleVal, DoubleVal, log10);
ONE_ARG_MATH_FN(Exp, DoubleVal, DoubleVal, exp);

FloatVal MathFunctions::Sign(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return FloatVal::null();
  return FloatVal((v.val > 0) ? 1.0f : ((v.val < 0) ? -1.0f : 0.0f));
}

DoubleVal MathFunctions::Radians(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(v.val * M_PI / 180.0);
}

DoubleVal MathFunctions::Degrees(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(v.val * 180.0 / M_PI);
}

BigIntVal MathFunctions::Round(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return BigIntVal::null();
  return BigIntVal(static_cast<int64_t>(v.val + ((v.val < 0) ? -0.5 : 0.5)));
}

DoubleVal MathFunctions::RoundUpTo(FunctionContext* ctx, const DoubleVal& v,
    const IntVal& scale) {
  if (v.is_null || scale.is_null) return DoubleVal::null();
  return DoubleVal(floor(v.val * pow(10.0, scale.val) + 0.5) / pow(10.0, scale.val));
}

DoubleVal MathFunctions::Log2(FunctionContext* ctx, const DoubleVal& v) {
  if (v.is_null) return DoubleVal::null();
  return DoubleVal(log(v.val) / log(2.0));
}

DoubleVal MathFunctions::Log(FunctionContext* ctx, const DoubleVal& base,
    const DoubleVal& v) {
  if (base.is_null || v.is_null) return DoubleVal::null();
  return DoubleVal(log(v.val) / log(base.val));
}

DoubleVal MathFunctions::Pow(FunctionContext* ctx, const DoubleVal& base,
    const DoubleVal& exp) {
  if (base.is_null || exp.is_null) return DoubleVal::null();
  return DoubleVal(pow(base.val, exp.val));
}

void MathFunctions::RandPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint32_t* seed = reinterpret_cast<uint32_t*>(ctx->Allocate(sizeof(uint32_t)));
    ctx->SetFunctionState(scope, seed);
    if (ctx->GetNumArgs() == 1) {
      // This is a call to RandSeed, initialize the seed
      // TODO: should we support non-constant seed?
      if (!ctx->IsArgConstant(0)) {
        ctx->SetError("Seed argument to rand() must be constant");
        return;
      }
      DCHECK_EQ(ctx->GetArgType(0)->type, FunctionContext::TYPE_BIGINT);
      BigIntVal* seed_arg = static_cast<BigIntVal*>(ctx->GetConstantArg(0));
      if (seed_arg->is_null) {
        seed = NULL;
      } else {
        *seed = seed_arg->val;
      }
    } else {
      // This is a call to Rand, initialize seed to 0
      // TODO: can we change this behavior? This is stupid.
      *seed = 0;
    }
  }
}

DoubleVal MathFunctions::Rand(FunctionContext* ctx) {
  uint32_t* seed =
      reinterpret_cast<uint32_t*>(ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(seed != NULL);
  *seed = rand_r(seed);
  // Normalize to [0,1].
  return DoubleVal(static_cast<double>(*seed) / RAND_MAX);
}

DoubleVal MathFunctions::RandSeed(FunctionContext* ctx, const BigIntVal& seed) {
  if (seed.is_null) return DoubleVal::null();
  return Rand(ctx);
}

StringVal MathFunctions::Bin(FunctionContext* ctx, const BigIntVal& v) {
  if (v.is_null) return StringVal::null();
  // Cast to an unsigned integer because it is compiler dependent
  // whether the sign bit will be shifted like a regular bit.
  // (logical vs. arithmetic shift for signed numbers)
  uint64_t n = static_cast<uint64_t>(v.val);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  const size_t max_bits = sizeof(uint64_t) * 8;
  char result[max_bits];
  uint32_t index = max_bits;
  do {
    result[--index] = '0' + (n & 1);
  } while (n >>= 1);
<<<<<<< HEAD
  StringValue val(result + index, max_bits - index);
  // Copies the data in result.
  e->result_.SetStringVal(val);
  return &e->result_.string_val;
}

void* MathFunctions::HexInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int64_t* num = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  stringstream ss;
  ss << hex << uppercase << *num;
  e->result_.SetStringVal(ss.str());
  return &e->result_.string_val;
}

void* MathFunctions::HexString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* s = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (s == NULL) return NULL;
  stringstream ss;
  ss << hex << uppercase << setw(2) << setfill('0');
  for (int i = 0; i < s->len; ++i) {
    ss << static_cast<int32_t>(s->ptr[i]);
  }
  e->result_.SetStringVal(ss.str());
  return &e->result_.string_val;
}

void* MathFunctions::Unhex(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* s = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (s == NULL) return NULL;
  // For uneven number of chars return empty string like Hive does.
  if (s->len % 2 != 0) {
    e->result_.string_val.len = 0;
    e->result_.string_val.ptr = NULL;
    return &e->result_.string_val;
  }
  int result_len = s->len / 2;
  char result[result_len];
  int res_index = 0;
  int s_index = 0;
  while (s_index < s->len) {
    char c = 0;
    for (int j = 0; j < 2; ++j, ++s_index) {
      switch(s->ptr[s_index]) {
=======
  return AnyValUtil::FromBuffer(ctx, result + index, max_bits - index);
}

StringVal MathFunctions::HexInt(FunctionContext* ctx, const BigIntVal& v) {
  if (v.is_null) return StringVal::null();
  // TODO: this is probably unreasonably slow
  stringstream ss;
  ss << hex << uppercase << v.val;
  return AnyValUtil::FromString(ctx, ss.str());
}

StringVal MathFunctions::HexString(FunctionContext* ctx, const StringVal& s) {
  if (s.is_null) return StringVal::null();
  stringstream ss;
  ss << hex << uppercase << setfill('0');
  for (int i = 0; i < s.len; ++i) {
    // setw is not sticky. stringstream only converts integral values,
    // so a cast to int is required, but only convert the least significant byte to hex.
    ss << setw(2) << (static_cast<int32_t>(s.ptr[i]) & 0xFF);
  }
  return AnyValUtil::FromString(ctx, ss.str());
}

StringVal MathFunctions::Unhex(FunctionContext* ctx, const StringVal& s) {
  if (s.is_null) return StringVal::null();
  // For uneven number of chars return empty string like Hive does.
  if (s.len % 2 != 0) return StringVal();

  int result_len = s.len / 2;
  char result[result_len];
  int res_index = 0;
  int s_index = 0;
  while (s_index < s.len) {
    char c = 0;
    for (int j = 0; j < 2; ++j, ++s_index) {
      switch(s.ptr[s_index]) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
<<<<<<< HEAD
          c += (s->ptr[s_index] - '0') * ((j == 0) ? 16 : 1);
=======
          c += (s.ptr[s_index] - '0') * ((j == 0) ? 16 : 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
          break;
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
          // Map to decimal values [10, 15]
<<<<<<< HEAD
          c += (s->ptr[s_index] - 'A' + 10) * ((j == 0) ? 16 : 1);
=======
          c += (s.ptr[s_index] - 'A' + 10) * ((j == 0) ? 16 : 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
          break;
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
          // Map to decimal [10, 15]
<<<<<<< HEAD
          c += (s->ptr[s_index] - 'a' + 10) * ((j == 0) ? 16 : 1);
          break;
        default:
          // Character not in hex alphabet, return empty string.
          e->result_.string_val.len = 0;
          e->result_.string_val.ptr = NULL;
          return &e->result_.string_val;
=======
          c += (s.ptr[s_index] - 'a' + 10) * ((j == 0) ? 16 : 1);
          break;
        default:
          // Character not in hex alphabet, return empty string.
          return StringVal();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      }
    }
    result[res_index] = c;
    ++res_index;
  }
<<<<<<< HEAD
  StringValue val(result, result_len);
  // Copies the data in result.
  e->result_.SetStringVal(val);
  return &e->result_.string_val;
}

void* MathFunctions::ConvInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  int64_t* num = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  int8_t* src_base = reinterpret_cast<int8_t*>(e->children()[1]->GetValue(row));
  int8_t* dest_base = reinterpret_cast<int8_t*>(e->children()[2]->GetValue(row));
  if (num == NULL || src_base == NULL || dest_base == NULL) {
    return NULL;
  }
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(*src_base) < MIN_BASE || abs(*src_base) > MAX_BASE
      || abs(*dest_base) < MIN_BASE || abs(*dest_base) > MAX_BASE) {
    // Return NULL like Hive does.
    return NULL;
  }
  if (*src_base < 0 && *num >= 0) {
    // Invalid input.
    return NULL;
  }
  int64_t decimal_num = *num;
  if (*src_base != 10) {
    // Convert src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    if (!DecimalInBaseToDecimal(*num, *src_base, &decimal_num)) {
      // Handle overflow, setting decimal_num appropriately.
      HandleParseResult(*dest_base, &decimal_num, StringParser::PARSE_OVERFLOW);
    }
  }
  DecimalToBase(decimal_num, *dest_base, &e->result_);
  return &e->result_.string_val;
}

void* MathFunctions::ConvString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* num_str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int8_t* src_base = reinterpret_cast<int8_t*>(e->children()[1]->GetValue(row));
  int8_t* dest_base = reinterpret_cast<int8_t*>(e->children()[2]->GetValue(row));
  if (num_str == NULL || src_base == NULL || dest_base == NULL) {
    return NULL;
  }
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(*src_base) < MIN_BASE || abs(*src_base) > MAX_BASE
      || abs(*dest_base) < MIN_BASE || abs(*dest_base) > MAX_BASE) {
    // Return NULL like Hive does.
    return NULL;
  }
  // Convert digits in num_str in src_base to decimal.
  StringParser::ParseResult parse_res;
  int64_t decimal_num = StringParser::StringToInt<int64_t>(num_str->ptr, num_str->len,
      *src_base, &parse_res);
  if (*src_base < 0 && decimal_num >= 0) {
    // Invalid input.
    return NULL;
  }
  if (!HandleParseResult(*dest_base, &decimal_num, parse_res)) {
    // Return 0 for invalid input strings like Hive does.
    StringValue val(const_cast<char*>("0"), 1);
    e->result_.SetStringVal(val);
    return &e->result_.string_val;
  }
  DecimalToBase(decimal_num, *dest_base, &e->result_);
  return &e->result_.string_val;
}

void MathFunctions::DecimalToBase(int64_t src_num, int8_t dest_base,
    ExprValue* expr_val) {
=======
  return AnyValUtil::FromBuffer(ctx, result, result_len);
}

StringVal MathFunctions::ConvInt(FunctionContext* ctx, const BigIntVal& num,
    const TinyIntVal& src_base, const TinyIntVal& dest_base) {
  if (num.is_null || src_base.is_null || dest_base.is_null) return StringVal::null();
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(src_base.val) < MIN_BASE || abs(src_base.val) > MAX_BASE
      || abs(dest_base.val) < MIN_BASE || abs(dest_base.val) > MAX_BASE) {
    // Return NULL like Hive does.
    return StringVal::null();
  }

  // Invalid input.
  if (src_base.val < 0 && num.val >= 0) return StringVal::null();
  int64_t decimal_num = num.val;
  if (src_base.val != 10) {
    // Convert src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    if (!DecimalInBaseToDecimal(num.val, src_base.val, &decimal_num)) {
      // Handle overflow, setting decimal_num appropriately.
      HandleParseResult(dest_base.val, &decimal_num, StringParser::PARSE_OVERFLOW);
    }
  }
  return DecimalToBase(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::ConvString(FunctionContext* ctx, const StringVal& num_str,
    const TinyIntVal& src_base, const TinyIntVal& dest_base) {
  if (num_str.is_null || src_base.is_null || dest_base.is_null) return StringVal::null();
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(src_base.val) < MIN_BASE || abs(src_base.val) > MAX_BASE
      || abs(dest_base.val) < MIN_BASE || abs(dest_base.val) > MAX_BASE) {
    // Return NULL like Hive does.
    return StringVal::null();
  }
  // Convert digits in num_str in src_base to decimal.
  StringParser::ParseResult parse_res;
  int64_t decimal_num = StringParser::StringToInt<int64_t>(
      reinterpret_cast<char*>(num_str.ptr), num_str.len, src_base.val, &parse_res);
  if (src_base.val < 0 && decimal_num >= 0) {
    // Invalid input.
    return StringVal::null();
  }
  if (!HandleParseResult(dest_base.val, &decimal_num, parse_res)) {
    // Return 0 for invalid input strings like Hive does.
    return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>("0")), 1);
  }
  return DecimalToBase(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::DecimalToBase(FunctionContext* ctx, int64_t src_num,
    int8_t dest_base) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Max number of digits of any base (base 2 gives max digits), plus sign.
  const size_t max_digits = sizeof(uint64_t) * 8 + 1;
  char buf[max_digits];
  int32_t result_len = 0;
  int32_t buf_index = max_digits - 1;
  uint64_t temp_num;
  if (dest_base < 0) {
    // Dest base is negative, treat src_num as signed.
    temp_num = abs(src_num);
  } else {
    // Dest base is positive. We must interpret src_num in 2's complement.
    // Convert to an unsigned int to properly deal with 2's complement conversion.
    temp_num = static_cast<uint64_t>(src_num);
  }
  int abs_base = abs(dest_base);
  do {
    buf[buf_index] = ALPHANUMERIC_CHARS[temp_num % abs_base];
    temp_num /= abs_base;
    --buf_index;
    ++result_len;
  } while (temp_num > 0);
  // Add optional sign.
  if (src_num < 0 && dest_base < 0) {
    buf[buf_index] = '-';
    ++result_len;
  }
<<<<<<< HEAD
  StringValue val(buf + max_digits - result_len, result_len);
  // Copies the data in result.
  expr_val->SetStringVal(val);
=======
  return AnyValUtil::FromBuffer(ctx, buf + max_digits - result_len, result_len);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

bool MathFunctions::DecimalInBaseToDecimal(int64_t src_num, int8_t src_base,
    int64_t* result) {
  uint64_t temp_num = abs(src_num);
  int32_t place = 1;
  *result = 0;
  do {
    int32_t digit = temp_num % 10;
    // Reset result if digit is not representable in src_base.
    if (digit >= src_base) {
      *result = 0;
      place = 1;
    } else {
      *result += digit * place;
      place *= src_base;
      // Overflow.
      if (UNLIKELY(*result < digit)) {
        return false;
      }
    }
    temp_num /= 10;
  } while (temp_num > 0);
  *result = (src_num < 0) ? -(*result) : *result;
  return true;
}

bool MathFunctions::HandleParseResult(int8_t dest_base, int64_t* num,
    StringParser::ParseResult parse_res) {
  // On overflow set special value depending on dest_base.
  // This is consistent with Hive and MySQL's behavior.
  if (parse_res == StringParser::PARSE_OVERFLOW) {
    if (dest_base < 0) {
      *num = -1;
    } else {
      *num = numeric_limits<uint64_t>::max();
    }
  } else if (parse_res == StringParser::PARSE_FAILURE) {
    // Some other error condition.
    return false;
  }
  return true;
}

<<<<<<< HEAD
void* MathFunctions::PmodBigInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  int64_t* a = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  int64_t* b = reinterpret_cast<int64_t*>(e->children()[1]->GetValue(row));
  if (a == NULL || b == NULL) return NULL;
  e->result_.bigint_val = ((*a % *b) + *b) % *b;
  return &e->result_.bigint_val;
}

void* MathFunctions::PmodDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* a = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* b = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (a == NULL || b == NULL) return NULL;
  e->result_.double_val = fmod(fmod(*a, *b) + *b, *b);
  return &e->result_.double_val;
}

void* MathFunctions::PositiveBigInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int64_t* i = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  if (i == NULL) return NULL;
  e->result_.bigint_val = *i;
  return &e->result_.bigint_val;
}

void* MathFunctions::PositiveDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = *d;
  return &e->result_.double_val;
}

void* MathFunctions::NegativeBigInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int64_t* i = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  if (i == NULL) return NULL;
  e->result_.bigint_val = -*i;
  return &e->result_.bigint_val;
}

void* MathFunctions::NegativeDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = -*d;
  return &e->result_.double_val;
}

}

=======
BigIntVal MathFunctions::PmodBigInt(FunctionContext* ctx, const BigIntVal& a,
    const BigIntVal& b) {
  if (a.is_null || b.is_null) return BigIntVal::null();
  return BigIntVal(((a.val % b.val) + b.val) % b.val);
}

DoubleVal MathFunctions::PmodDouble(FunctionContext* ctx, const DoubleVal& a,
    const DoubleVal& b) {
  if (a.is_null || b.is_null) return DoubleVal::null();
  return DoubleVal(fmod(fmod(a.val, b.val) + b.val, b.val));
}

FloatVal MathFunctions::FmodFloat(FunctionContext* ctx, const FloatVal& a,
    const FloatVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return FloatVal::null();
  return FloatVal(fmodf(a.val, b.val));
}

DoubleVal MathFunctions::FmodDouble(FunctionContext* ctx, const DoubleVal& a,
    const DoubleVal& b) {
  if (a.is_null || b.is_null || b.val == 0) return DoubleVal::null();
  return DoubleVal(fmod(a.val, b.val));
}

template <typename T> T MathFunctions::Positive(FunctionContext* ctx, const T& val) {
  return val;
}

template <typename T> T MathFunctions::Negative(FunctionContext* ctx, const T& val) {
  if (val.is_null) return val;
  return T(-val.val);
}

template <>
DecimalVal MathFunctions::Negative(FunctionContext* ctx, const DecimalVal& val) {
  if (val.is_null) return val;
  return DecimalVal(-val.val16);
}

BigIntVal MathFunctions::QuotientDouble(FunctionContext* ctx, const DoubleVal& x,
    const DoubleVal& y) {
  if (x.is_null || y.is_null || static_cast<int64_t>(y.val)  == 0) {
    return BigIntVal::null();
  }
  return BigIntVal(static_cast<int64_t>(x.val) / static_cast<int64_t>(y.val));
}

BigIntVal MathFunctions::QuotientBigInt(FunctionContext* ctx, const BigIntVal& x,
    const BigIntVal& y) {
  return Operators::Int_divide_BigIntVal_BigIntVal(ctx, x, y);
}

template <typename VAL_TYPE, bool ISLEAST> VAL_TYPE MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const VAL_TYPE* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return VAL_TYPE::null();
  int result_idx = 0;
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return VAL_TYPE::null();
    if (ISLEAST) {
      if (args[i].val < args[result_idx].val) result_idx = i;
    } else {
      if (args[i].val > args[result_idx].val) result_idx = i;
    }
  }
  return VAL_TYPE(args[result_idx].val);
}

template <bool ISLEAST> StringVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const StringVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return StringVal::null();
  StringValue result_val = StringValue::FromStringVal(args[0]);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return StringVal::null();
    StringValue val = StringValue::FromStringVal(args[i]);
    if (ISLEAST) {
      if (val < result_val) result_val = val;
    } else {
      if (val > result_val) result_val = val;
    }
  }
  StringVal result;
  result_val.ToStringVal(&result);
  return result;
}

template <bool ISLEAST> TimestampVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const TimestampVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return TimestampVal::null();
  TimestampValue result_val = TimestampValue::FromTimestampVal(args[0]);
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return TimestampVal::null();
    TimestampValue val = TimestampValue::FromTimestampVal(args[i]);
    if (ISLEAST) {
      if (val < result_val) result_val = val;
    } else {
      if (val > result_val) result_val = val;
    }
  }
  TimestampVal result;
  result_val.ToTimestampVal(&result);
  return result;
}

template <bool ISLEAST> DecimalVal MathFunctions::LeastGreatest(
    FunctionContext* ctx, int num_args, const DecimalVal* args) {
  DCHECK_GT(num_args, 0);
  if (args[0].is_null) return DecimalVal::null();
  DecimalVal result_val = args[0];
  for (int i = 1; i < num_args; ++i) {
    if (args[i].is_null) return DecimalVal::null();
    if (ISLEAST) {
      if (args[i].val16 < result_val.val16) result_val = args[i];
    } else {
      if (args[i].val16 > result_val.val16) result_val = args[i];
    }
  }
  return result_val;
}

template TinyIntVal MathFunctions::Positive<TinyIntVal>(
    FunctionContext* ctx, const TinyIntVal& val);
template SmallIntVal MathFunctions::Positive<SmallIntVal>(
    FunctionContext* ctx, const SmallIntVal& val);
template IntVal MathFunctions::Positive<IntVal>(
    FunctionContext* ctx, const IntVal& val);
template BigIntVal MathFunctions::Positive<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& val);
template FloatVal MathFunctions::Positive<FloatVal>(
    FunctionContext* ctx, const FloatVal& val);
template DoubleVal MathFunctions::Positive<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& val);
template DecimalVal MathFunctions::Positive<DecimalVal>(
    FunctionContext* ctx, const DecimalVal& val);

template TinyIntVal MathFunctions::Negative<TinyIntVal>(
    FunctionContext* ctx, const TinyIntVal& val);
template SmallIntVal MathFunctions::Negative<SmallIntVal>(
    FunctionContext* ctx, const SmallIntVal& val);
template IntVal MathFunctions::Negative<IntVal>(
    FunctionContext* ctx, const IntVal& val);
template BigIntVal MathFunctions::Negative<BigIntVal>(
    FunctionContext* ctx, const BigIntVal& val);
template FloatVal MathFunctions::Negative<FloatVal>(
    FunctionContext* ctx, const FloatVal& val);
template DoubleVal MathFunctions::Negative<DoubleVal>(
    FunctionContext* ctx, const DoubleVal& val);

template TinyIntVal MathFunctions::LeastGreatest<TinyIntVal, true>(
    FunctionContext* ctx, int num_args, const TinyIntVal* args);
template SmallIntVal MathFunctions::LeastGreatest<SmallIntVal, true>(
    FunctionContext* ctx, int num_args, const SmallIntVal* args);
template IntVal MathFunctions::LeastGreatest<IntVal, true>(
    FunctionContext* ctx, int num_args, const IntVal* args);
template BigIntVal MathFunctions::LeastGreatest<BigIntVal, true>(
    FunctionContext* ctx, int num_args, const BigIntVal* args);
template FloatVal MathFunctions::LeastGreatest<FloatVal, true>(
    FunctionContext* ctx, int num_args, const FloatVal* args);
template DoubleVal MathFunctions::LeastGreatest<DoubleVal, true>(
    FunctionContext* ctx, int num_args, const DoubleVal* args);

template TinyIntVal MathFunctions::LeastGreatest<TinyIntVal, false>(
    FunctionContext* ctx, int num_args, const TinyIntVal* args);
template SmallIntVal MathFunctions::LeastGreatest<SmallIntVal, false>(
    FunctionContext* ctx, int num_args, const SmallIntVal* args);
template IntVal MathFunctions::LeastGreatest<IntVal, false>(
    FunctionContext* ctx, int num_args, const IntVal* args);
template BigIntVal MathFunctions::LeastGreatest<BigIntVal, false>(
    FunctionContext* ctx, int num_args, const BigIntVal* args);
template FloatVal MathFunctions::LeastGreatest<FloatVal, false>(
    FunctionContext* ctx, int num_args, const FloatVal* args);
template DoubleVal MathFunctions::LeastGreatest<DoubleVal, false>(
    FunctionContext* ctx, int num_args, const DoubleVal* args);

template StringVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const StringVal* args);
template StringVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const StringVal* args);

template TimestampVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const TimestampVal* args);
template TimestampVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const TimestampVal* args);

template DecimalVal MathFunctions::LeastGreatest<true>(
    FunctionContext* ctx, int num_args, const DecimalVal* args);
template DecimalVal MathFunctions::LeastGreatest<false>(
    FunctionContext* ctx, int num_args, const DecimalVal* args);

}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
