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

#include "exprs/string-functions.h"

<<<<<<< HEAD
#include <boost/regex.hpp>

#include "exprs/expr.h"
#include "exprs/function-call.h"
=======
#include <cctype>
#include <stdint.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "exprs/anyval-util.h"
#include "exprs/expr.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/url-parser.h"

<<<<<<< HEAD
using namespace std;
using namespace boost;

namespace impala {

// Implementation of Substr.  The signature is
//    string substr(string input, int pos, int len)
=======
using namespace boost;
using namespace impala_udf;
using namespace std;

// NOTE: be careful not to use string::append.  It is not performant.
namespace impala {

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
<<<<<<< HEAD
void* StringFunctions::Substring(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  Expr* op3 = NULL;
  if (e->GetNumChildren() == 3) op3 = e->children()[2];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* pos = reinterpret_cast<int*>(op2->GetValue(row));
  int* len = op3 != NULL ? reinterpret_cast<int*>(op3->GetValue(row)) : NULL;
  if (str == NULL || pos == NULL || (op3 != NULL && len == NULL)) return NULL;
  string tmp(str->ptr, str->len);
  int fixed_pos = *pos;
  int fixed_len = (len == NULL ? str->len : *len);
  string result;
  if (fixed_pos < 0) fixed_pos = str->len + fixed_pos + 1;
  if (fixed_pos > 0 && fixed_pos <= str->len && fixed_len > 0) {
    result = tmp.substr(fixed_pos - 1, fixed_len);
  }
  e->result_.SetStringVal(result);
  return &e->result_.string_val;
}

// Implementation of Left.  The signature is
//    string left(string input, int len)
// This behaves identically to the mysql implementation.
void* StringFunctions::Left(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* len = reinterpret_cast<int*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  return &e->result_.string_val;
}

// Implementation of Right.  The signature is
//    string right(string input, int len)
// This behaves identically to the mysql implementation.
void* StringFunctions::Right(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  StringValue* str = reinterpret_cast<StringValue*>(op1->GetValue(row));
  int* len = reinterpret_cast<int*>(op2->GetValue(row));
  if (str == NULL || len == NULL) return NULL;
  e->result_.string_val.len = str->len <= *len ? str->len : *len;
  e->result_.string_val.ptr = str->ptr;
  if (str->len > *len) e->result_.string_val.ptr += str->len - *len;
  return &e->result_.string_val;
}

// Implementation of LENGTH
//   int length(string input)
// Returns the length in bytes of input. If input == NULL, returns
// NULL per MySQL
void* StringFunctions::Length(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.int_val = str->len;
  return &e->result_.int_val;
}

// Implementation of LOWER
//   string lower(string input)
// Returns a string identical to the input, but with all characters
// mapped to their lower-case equivalents. If input == NULL, returns
// NULL per MySQL.
// Both this function, and Upper, have opportunities for SIMD-based
// optimization (since the current implementation operates on one
// character at a time).
void* StringFunctions::Lower(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  // Writing to string_val.ptr rather than string_data.begin() ensures
  // that we bypass any range checks on the output iterator (since we
  // know for sure that we're writing str->len bytes)
  std::transform(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr, ::tolower);

  return &e->result_.string_val;
}

// Implementation of UPPER
//   string upper(string input)
// Returns a string identical to the input, but with all characters
// mapped to their upper-case equivalents. If input == NULL, returns
// NULL per MySQL
void* StringFunctions::Upper(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  // Writing to string_val.ptr rather than string_data.begin() ensures
  // that we bypass any range checks on the output iterator (since we
  // know for sure that we're writing str->len bytes)
  std::transform(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr, ::toupper);

  return &e->result_.string_val;
}

// Implementation of REVERSE
//   string upper(string input)
// Returns a string with caracters in reverse order to the input.
// If input == NULL, returns NULL per MySQL
void* StringFunctions::Reverse(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  StringValue* str = reinterpret_cast<StringValue*>(op->GetValue(row));
  if (str == NULL) return NULL;

  e->result_.string_data.resize(str->len);
  e->result_.SyncStringVal();

  std::reverse_copy(str->ptr, str->ptr + str->len,
                 e->result_.string_val.ptr);

  return (&e->result_.string_val);
}

void* StringFunctions::Trim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str->len && str->ptr[begin] == ' ') {
    ++begin;
  }
  // Find new ending position.
  int32_t end = str->len - 1;
  while (end > begin && str->ptr[end] == ' ') {
    --end;
  }
  e->result_.string_val.ptr = str->ptr + begin;
  e->result_.string_val.len = end - begin + 1;
  return &e->result_.string_val;
}

void* StringFunctions::Ltrim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str->len && str->ptr[begin] == ' ') {
    ++begin;
  }
  e->result_.string_val.ptr = str->ptr + begin;
  e->result_.string_val.len = str->len - begin;
  return &e->result_.string_val;
}

void* StringFunctions::Rtrim(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Find new ending position.
  int32_t end = str->len - 1;
  while (end > 0 && str->ptr[end] == ' ') {
    --end;
  }
  e->result_.string_val.ptr = str->ptr;
  e->result_.string_val.len = (str->ptr[end] == ' ') ? end : end + 1;
  return &e->result_.string_val;
}

void* StringFunctions::Space(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int32_t* num = reinterpret_cast<int32_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  if (*num <= 0) {
    e->result_.string_val.ptr = NULL;
    e->result_.string_val.len = 0;
    return &e->result_.string_val;
  }
  e->result_.string_data = string(*num, ' ');
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Repeat(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* num = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  if (num == NULL || str == NULL) return NULL;
  if (str->len == 0 || *num <= 0) {
    e->result_.string_val.ptr = NULL;
    e->result_.string_val.len = 0;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(str->len * (*num));
  for (int32_t i = 0; i < *num; ++i) {
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Ascii(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (str == NULL) return NULL;
  // Hive returns 0 when given an empty string.
  e->result_.int_val = (str->len == 0) ? 0 : static_cast<int32_t>(str->ptr[0]);
  return &e->result_.int_val;
}

void* StringFunctions::Lpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* len = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(*len);
  // Prepend chars of pad.
  int padded_prefix_len = *len - str->len;
  int pad_index = 0;
  while (e->result_.string_data.length() < padded_prefix_len) {
    e->result_.string_data.append(1, pad->ptr[pad_index]);
    ++pad_index;
    pad_index = pad_index % pad->len;
  }
  // Append given string.
  e->result_.string_data.append(str->ptr, str->len);
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Rpad(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int32_t* len = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  StringValue* pad = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || len == NULL || pad == NULL) return NULL;
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (*len <= str->len || pad->len == 0) {
    e->result_.string_val.ptr = str->ptr;
    e->result_.string_val.len = *len;
    return &e->result_.string_val;
  }
  e->result_.string_data.reserve(*len);
  e->result_.string_data.append(str->ptr, str->len);
  // Append chars of pad until desired length.
  int pad_index = 0;
  while (e->result_.string_data.length() < *len) {
    e->result_.string_data.append(1, pad->ptr[pad_index]);
    ++pad_index;
    pad_index = pad_index % pad->len;
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Instr(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || substr == NULL) return NULL;
  StringSearch search(substr);
  // Hive returns positions starting from 1.
  e->result_.int_val = search.Search(str) + 1;
  return &e->result_.int_val;
}

void* StringFunctions::Locate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || substr == NULL) return NULL;
  StringSearch search(substr);
  // Hive returns positions starting from 1.
  e->result_.int_val = search.Search(str) + 1;
  return &e->result_.int_val;
}

void* StringFunctions::LocatePos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* substr = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  int32_t* start_pos = reinterpret_cast<int32_t*>(e->children()[2]->GetValue(row));
  if (str == NULL || substr == NULL || start_pos == NULL) return NULL;
  // Hive returns 0 for *start_pos <= 0,
  // but throws an exception for *start_pos > str->len.
  // Since returning 0 seems to be Hive's error condition, return 0.
  if (*start_pos <= 0 || *start_pos > str->len) {
    e->result_.int_val = 0;
    return &e->result_.int_val;
  }
  StringSearch search(substr);
  // Input start_pos starts from 1.
  StringValue adjusted_str(str->ptr + *start_pos - 1, str->len - *start_pos + 1);
  int32_t match_pos = search.Search(&adjusted_str);
  if (match_pos >= 0) {
    // Hive returns the position in the original string starting from 1.
    e->result_.int_val = *start_pos + match_pos;
  } else {
    e->result_.int_val = 0;
  }
  return &e->result_.int_val;
}

void* StringFunctions::RegexpExtract(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* pattern = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  int32_t* index = reinterpret_cast<int32_t*>(e->children()[2]->GetValue(row));
  if (str == NULL || pattern == NULL || index == NULL) return NULL;
  FunctionCall* func_expr = static_cast<FunctionCall*>(e);
  // Compile the regex if pattern is not constant,
  // or if pattern is constant and this is the first function invocation.
  if ((!e->children()[1]->IsConstant()) ||
      (e->children()[1]->IsConstant() && func_expr->GetRegex() == NULL)) {
    string pattern_str(pattern->ptr, pattern->len);
    bool valid_pattern = func_expr->SetRegex(pattern_str);
    // Hive throws an exception for invalid patterns.
    if (!valid_pattern) {
      return NULL;
    }
  }
  DCHECK(func_expr->GetRegex() != NULL);
  cmatch matches;
  // cast's are necessary to make boost understand which function we want.
  bool success = regex_search(const_cast<const char*>(str->ptr),
      const_cast<const char*>(str->ptr) + str->len,
      matches, *func_expr->GetRegex(), regex_constants::match_any);
  if (!success) {
    e->result_.SetStringVal("");
    return &e->result_.string_val;
  }
  // match[0] is the whole string, match_res.str(1) the first group, etc.
  e->result_.SetStringVal(matches[*index]);
  return &e->result_.string_val;
}

void* StringFunctions::RegexpReplace(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* pattern = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  StringValue* replace = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (str == NULL || pattern == NULL || replace == NULL) return NULL;
  FunctionCall* func_expr = static_cast<FunctionCall*>(e);
  // Compile the regex if pattern is not constant,
  // or if pattern is constant and this is the first function invocation.
  if ((!e->children()[1]->IsConstant()) ||
      (e->children()[1]->IsConstant() && func_expr->GetRegex() == NULL)) {
    string pattern_str(pattern->ptr, pattern->len);
    bool valid_pattern = func_expr->SetRegex(pattern_str);
    // Hive throws an exception for invalid patterns.
    if (!valid_pattern) {
      return NULL;
    }
  }
  DCHECK(func_expr->GetRegex() != NULL);
  // Only copy replace if it is not constant, or if it constant
  // and this is the first invocation.
  if ((!e->children()[2]->IsConstant()) ||
      (e->children()[2]->IsConstant() && func_expr->GetReplaceStr() == NULL)) {
    func_expr->SetReplaceStr(replace);
  }
  DCHECK(func_expr->GetReplaceStr() != NULL);
  e->result_.string_data.clear();
  // cast's are necessary to make boost understand which function we want.
  re_detail::string_out_iterator<basic_string<char> >
      out_iter(e->result_.string_data);
  regex_replace(out_iter, const_cast<const char*>(str->ptr),
      const_cast<const char*>(str->ptr) + str->len, *func_expr->GetRegex(),
      *func_expr->GetReplaceStr());
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::Concat(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 1);
  e->result_.string_data.clear();
  int32_t total_size = 0;
  int32_t num_children = e->GetNumChildren();
  // Loop once to compute the final size and reserve space.
  for (int32_t i = 0; i < num_children; ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    total_size += str->len;
  }
  e->result_.string_data.reserve(total_size);
  // Loop again to append the data.
  for (int32_t i = 0; i < e->GetNumChildren(); ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::ConcatWs(Expr* e, TupleRow* row) {
  DCHECK_GE(e->GetNumChildren(), 2);
  StringValue* sep = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (sep == NULL) return NULL;
  StringValue* first = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (first == NULL) return NULL;
  e->result_.string_data.clear();
  int32_t total_size = first->len;
  int32_t num_children = e->GetNumChildren();
  // Loop once to compute the final size and reserve space.
  for (int32_t i = 2; i < num_children; ++i) {
      StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
      if (str == NULL) return NULL;
      total_size += sep->len + str->len;
    }
  e->result_.string_data.reserve(total_size);
  // Loop again to append the data.
  e->result_.string_data.append(first->ptr, first->len);
  for (int32_t i = 2; i < num_children; ++i) {
    StringValue* str = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (str == NULL) return NULL;
    e->result_.string_data.append(sep->ptr, sep->len);
    e->result_.string_data.append(str->ptr, str->len);
  }
  e->result_.SyncStringVal();
  return &e->result_.string_val;
}

void* StringFunctions::FindInSet(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* str_set = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (str == NULL || str_set == NULL) return NULL;
  // Check str for commas.
  for (int i = 0; i < str->len; ++i) {
    if (str->ptr[i] == ',') {
      e->result_.int_val = 0;
      return &e->result_.int_val;
    }
=======
StringVal StringFunctions::Substring(FunctionContext* context,
    const StringVal& str, const BigIntVal& pos, const BigIntVal& len) {
  if (str.is_null || pos.is_null || len.is_null) return StringVal::null();
  int fixed_pos = pos.val;
  if (fixed_pos < 0) fixed_pos = str.len + fixed_pos + 1;
  int max_len = str.len - fixed_pos + 1;
  int fixed_len = ::min(static_cast<int>(len.val), max_len);
  if (fixed_pos > 0 && fixed_pos <= str.len && fixed_len > 0) {
    return StringVal(str.ptr + fixed_pos - 1, fixed_len);
  } else {
    return StringVal();
  }
}

StringVal StringFunctions::Substring(FunctionContext* context,
    const StringVal& str, const BigIntVal& pos) {
  // StringVal.len is an int => INT32_MAX
  return Substring(context, str, pos, BigIntVal(INT32_MAX));
}

// This behaves identically to the mysql implementation.
StringVal StringFunctions::Left(
    FunctionContext* context, const StringVal& str, const BigIntVal& len) {
  return Substring(context, str, 1, len);
}

// This behaves identically to the mysql implementation.
StringVal StringFunctions::Right(
    FunctionContext* context, const StringVal& str, const BigIntVal& len) {
  // Don't index past the beginning of str, otherwise we'll get an empty string back
  int64_t pos = ::max(-len.val, static_cast<int64_t>(-str.len));
  return Substring(context, str, BigIntVal(pos), len);
}

StringVal StringFunctions::Space(FunctionContext* context, const BigIntVal& len) {
  if (len.is_null) return StringVal::null();
  if (len.val <= 0) return StringVal();
  StringVal result(context, len.val);
  memset(result.ptr, ' ', len.val);
  return result;
}

StringVal StringFunctions::Repeat(
    FunctionContext* context, const StringVal& str, const BigIntVal& n) {
  if (str.is_null || n.is_null) return StringVal::null();
  if (str.len == 0 || n.val <= 0) return StringVal();
  StringVal result(context, str.len * n.val);
  uint8_t* ptr = result.ptr;
  for (int64_t i = 0; i < n.val; ++i) {
    memcpy(ptr, str.ptr, str.len);
    ptr += str.len;
  }
  return result;
}

StringVal StringFunctions::Lpad(FunctionContext* context, const StringVal& str,
    const BigIntVal& len, const StringVal& pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) return StringVal::null();
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad.len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (len.val <= str.len || pad.len == 0) return StringVal(str.ptr, len.val);

  StringVal result(context, len.val);
  int padded_prefix_len = len.val - str.len;
  int pad_index = 0;
  int result_index = 0;
  uint8_t* ptr = result.ptr;

  // Prepend chars of pad.
  while (result_index < padded_prefix_len) {
    ptr[result_index++] = pad.ptr[pad_index++];
    pad_index = pad_index % pad.len;
  }

  // Append given string.
  memcpy(ptr + result_index, str.ptr, str.len);
  return result;
}

StringVal StringFunctions::Rpad(FunctionContext* context, const StringVal& str,
    const BigIntVal& len, const StringVal& pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) return StringVal::null();
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (len.val <= str.len || pad.len == 0) {
    return StringVal(str.ptr, len.val);
  }

  StringVal result(context, len.val);
  memcpy(result.ptr, str.ptr, str.len);

  // Append chars of pad until desired length
  uint8_t* ptr = result.ptr;
  int pad_index = 0;
  int result_len = str.len;
  while (result_len < len.val) {
    ptr[result_len++] = pad.ptr[pad_index++];
    pad_index = pad_index % pad.len;
  }
  return result;
}

IntVal StringFunctions::Length(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  return IntVal(str.len);
}

IntVal StringFunctions::CharLength(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  const FunctionContext::TypeDesc* t = context->GetArgType(0);
  DCHECK_EQ(t->type, FunctionContext::TYPE_FIXED_BUFFER);
  return StringValue::UnpaddedCharLength(reinterpret_cast<char*>(str.ptr), t->len);
}

StringVal StringFunctions::Lower(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::tolower(str.ptr[i]);
  }
  return result;
}

StringVal StringFunctions::Upper(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::toupper(str.ptr[i]);
  }
  return result;
}

// Returns a string identical to the input, but with the first character
// of each word mapped to its upper-case equivalent. All other characters
// will be mapped to their lower-case equivalents. If input == NULL it
// will return NULL
StringVal StringFunctions::InitCap(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  uint8_t* result_ptr = result.ptr;
  bool word_start = true;
  for (int i = 0; i < str.len; ++i) {
    if (isspace(str.ptr[i])) {
      result_ptr[i] = str.ptr[i];
      word_start = true;
    } else {
      result_ptr[i] = (word_start ? toupper(str.ptr[i]) : tolower(str.ptr[i]));
      word_start = false;
    }
  }
  return result;
}

StringVal StringFunctions::Reverse(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  std::reverse_copy(str.ptr, str.ptr + str.len, result.ptr);
  return result;
}

StringVal StringFunctions::Translate(FunctionContext* context, const StringVal& str,
    const StringVal& src, const StringVal& dst) {
  if (str.is_null || src.is_null || dst.is_null) return StringVal::null();
  StringVal result(context, str.len);

  // TODO: if we know src and dst are constant, we can prebuild a conversion
  // table to remove the inner loop.
  int result_len = 0;
  for (int i = 0; i < str.len; ++i) {
    bool matched_src = false;
    for (int j = 0; j < src.len; ++j) {
      if (str.ptr[i] == src.ptr[j]) {
        if (j < dst.len) {
          result.ptr[result_len++] = dst.ptr[j];
        } else {
          // src[j] doesn't map to any char in dst, the char is dropped.
        }
        matched_src = true;
        break;
      }
    }
    if (!matched_src) result.ptr[result_len++] = str.ptr[i];
  }
  result.len = result_len;
  return result;
}

StringVal StringFunctions::Trim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str.len && str.ptr[begin] == ' ') {
    ++begin;
  }
  // Find new ending position.
  int32_t end = str.len - 1;
  while (end > begin && str.ptr[end] == ' ') {
    --end;
  }
  return StringVal(str.ptr + begin, end - begin + 1);
}

StringVal StringFunctions::Ltrim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str.len && str.ptr[begin] == ' ') {
    ++begin;
  }
  return StringVal(str.ptr + begin, str.len - begin);
}

StringVal StringFunctions::Rtrim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (str.len == 0) return str;
  // Find new ending position.
  int32_t end = str.len - 1;
  while (end > 0 && str.ptr[end] == ' ') {
    --end;
  }
  DCHECK_GE(end, 0);
  return StringVal(str.ptr, (str.ptr[end] == ' ') ? end : end + 1);
}

IntVal StringFunctions::Ascii(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  // Hive returns 0 when given an empty string.
  return IntVal((str.len == 0) ? 0 : static_cast<int32_t>(str.ptr[0]));
}

IntVal StringFunctions::Instr(FunctionContext* context, const StringVal& str,
    const StringVal& substr) {
  if (str.is_null || substr.is_null) return IntVal::null();
  StringValue str_sv = StringValue::FromStringVal(str);
  StringValue substr_sv = StringValue::FromStringVal(substr);
  StringSearch search(&substr_sv);
  // Hive returns positions starting from 1.
  return IntVal(search.Search(&str_sv) + 1);
}

IntVal StringFunctions::Locate(FunctionContext* context, const StringVal& substr,
    const StringVal& str) {
  return Instr(context, str, substr);
}

IntVal StringFunctions::LocatePos(FunctionContext* context, const StringVal& substr,
    const StringVal& str, const BigIntVal& start_pos) {
  if (str.is_null || substr.is_null || start_pos.is_null) return IntVal::null();
  // Hive returns 0 for *start_pos <= 0,
  // but throws an exception for *start_pos > str->len.
  // Since returning 0 seems to be Hive's error condition, return 0.
  if (start_pos.val <= 0 || start_pos.val > str.len) return IntVal(0);
  StringValue substr_sv = StringValue::FromStringVal(substr);
  StringSearch search(&substr_sv);
  // Input start_pos.val starts from 1.
  StringValue adjusted_str(reinterpret_cast<char*>(str.ptr) + start_pos.val - 1,
                           str.len - start_pos.val + 1);
  int32_t match_pos = search.Search(&adjusted_str);
  if (match_pos >= 0) {
    // Hive returns the position in the original string starting from 1.
    return IntVal(start_pos.val + match_pos);
  } else {
    return IntVal(0);
  }
}

// The caller owns the returned regex. Returns NULL if the pattern could not be compiled.
re2::RE2* CompileRegex(const StringVal& pattern, string* error_str) {
  re2::StringPiece pattern_sp(reinterpret_cast<char*>(pattern.ptr), pattern.len);
  re2::RE2::Options options;
  // Disable error logging in case e.g. every row causes an error
  options.set_log_errors(false);
  // Return the leftmost longest match (rather than the first match).
  options.set_longest_match(true);
  re2::RE2* re = new re2::RE2(pattern_sp, options);
  if (!re->ok()) {
    stringstream ss;
    ss << "Could not compile regexp pattern: " << AnyValUtil::ToString(pattern) << endl
       << "Error: " << re->error();
    *error_str = ss.str();
    delete re;
    return NULL;
  }
  return re;
}

void StringFunctions::RegexpPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (pattern->is_null) return;

  string error_str;
  re2::RE2* re = CompileRegex(*pattern, &error_str);
  if (re == NULL) {
    context->SetError(error_str.c_str());
    return;
  }
  context->SetFunctionState(scope, re);
}

void StringFunctions::RegexpClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  re2::RE2* re = reinterpret_cast<re2::RE2*>(context->GetFunctionState(scope));
  delete re;
}

StringVal StringFunctions::RegexpExtract(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const BigIntVal& index) {
  if (str.is_null || pattern.is_null || index.is_null) return StringVal::null();
  if (index.val < 0) return StringVal();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if we have to locally compile it
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str);
    if (re == NULL) {
      context->AddWarning(error_str.c_str());
      return StringVal::null();
    }
    scoped_re.reset(re);
  }

  re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
  int max_matches = 1 + re->NumberOfCapturingGroups();
  if (index.val >= max_matches) return StringVal();
  // Use a vector because clang complains about non-POD varlen arrays
  // TODO: fix this
  vector<re2::StringPiece> matches(max_matches);
  bool success =
      re->Match(str_sp, 0, str.len, re2::RE2::UNANCHORED, &matches[0], max_matches);
  if (!success) return StringVal();
  // matches[0] is the whole string, matches[1] the first group, etc.
  const re2::StringPiece& match = matches[index.val];
  return AnyValUtil::FromBuffer(context, match.data(), match.size());
}

StringVal StringFunctions::RegexpReplace(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const StringVal& replace) {
  if (str.is_null || pattern.is_null || replace.is_null) return StringVal::null();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if state->re is NULL
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str);
    if (re == NULL) {
      context->AddWarning(error_str.c_str());
      return StringVal::null();
    }
    scoped_re.reset(re);
  }

  re2::StringPiece replace_str =
      re2::StringPiece(reinterpret_cast<char*>(replace.ptr), replace.len);
  string result_str = AnyValUtil::ToString(str);
  re2::RE2::GlobalReplace(&result_str, *re, replace_str);
  return AnyValUtil::FromString(context, result_str);
}

StringVal StringFunctions::Concat(FunctionContext* context, int num_children,
    const StringVal* strs) {
  return ConcatWs(context, StringVal(), num_children, strs);
}

StringVal StringFunctions::ConcatWs(FunctionContext* context, const StringVal& sep,
    int num_children, const StringVal* strs) {
  DCHECK_GE(num_children, 1);
  if (sep.is_null) return StringVal::null();

  // Pass through if there's only one argument
  if (num_children == 1) return strs[0];

  if (strs[0].is_null) return StringVal::null();
  int32_t total_size = strs[0].len;

  // Loop once to compute the final size and reserve space.
  for (int32_t i = 1; i < num_children; ++i) {
    if (strs[i].is_null) return StringVal::null();
    total_size += sep.len + strs[i].len;
  }
  StringVal result(context, total_size);
  uint8_t* ptr = result.ptr;

  // Loop again to append the data.
  memcpy(ptr, strs[0].ptr, strs[0].len);
  ptr += strs[0].len;
  for (int32_t i = 1; i < num_children; ++i) {
    memcpy(ptr, sep.ptr, sep.len);
    ptr += sep.len;
    memcpy(ptr, strs[i].ptr, strs[i].len);
    ptr += strs[i].len;
  }
  return result;
}

IntVal StringFunctions::FindInSet(FunctionContext* context, const StringVal& str,
    const StringVal& str_set) {
  if (str.is_null || str_set.is_null) return IntVal::null();
  // Check str for commas.
  for (int i = 0; i < str.len; ++i) {
    if (str.ptr[i] == ',') return IntVal(0);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  // The result index starts from 1 since 0 is an error condition.
  int32_t token_index = 1;
  int32_t start = 0;
  int32_t end;
<<<<<<< HEAD
  do {
    end = start;
    // Position end.
    while(str_set->ptr[end] != ',' && end < str_set->len) ++end;
    StringValue token(str_set->ptr + start, end - start);
    if (str->Eq(token)) {
      e->result_.int_val = token_index;
      return &e->result_.int_val;
    }
    // Re-position start and end past ','
    start = end + 1;
    ++token_index;
  } while (start < str_set->len);
  e->result_.int_val = 0;
  return &e->result_.int_val;
}

void* StringFunctions::ParseUrl(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  StringValue* url = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* part = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  if (url == NULL || part == NULL) return NULL;
  // We use e->result_.bool_val to indicate whether this is the first invocation.
  // Use e->result_.int_val to hold the URL part enum.
  if (!e->children()[1]->IsConstant() ||
      (e->children()[1]->IsConstant() && !e->result_.bool_val)) {
    e->result_.int_val = UrlParser::GetUrlPart(part);
    e->result_.bool_val = true;
  }
  UrlParser::UrlPart url_part = static_cast<UrlParser::UrlPart>(e->result_.int_val);
  if (!UrlParser::ParseUrl(url, url_part, &e->result_.string_val)) {
    // url is malformed, or url_part is invalid.
    return NULL;
  }
  return &e->result_.string_val;
}

void* StringFunctions::ParseUrlKey(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* url = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  StringValue* part = reinterpret_cast<StringValue*>(e->children()[1]->GetValue(row));
  StringValue* key = reinterpret_cast<StringValue*>(e->children()[2]->GetValue(row));
  if (url == NULL || part == NULL || key == NULL) return NULL;
  // We use e->result_.bool_val to indicate whether this is the first invocation.
  // Use e->result_.int_val to hold the URL part.
  if (!e->children()[1]->IsConstant() ||
      (e->children()[1]->IsConstant() && !e->result_.bool_val)) {
    e->result_.int_val = UrlParser::GetUrlPart(part);
    e->result_.bool_val = true;
  }
  UrlParser::UrlPart url_part = static_cast<UrlParser::UrlPart>(e->result_.int_val);
  if (!UrlParser::ParseUrlKey(url, url_part, key, &e->result_.string_val)) {
    // url is malformed, or url_part is not QUERY.
    return NULL;
  }
  return &e->result_.string_val;
=======
  StringValue str_sv = StringValue::FromStringVal(str);
  do {
    end = start;
    // Position end.
    while(str_set.ptr[end] != ',' && end < str_set.len) ++end;
    StringValue token(reinterpret_cast<char*>(str_set.ptr) + start, end - start);
    if (str_sv.Eq(token)) return IntVal(token_index);

    // Re-position start and end past ','
    start = end + 1;
    ++token_index;
  } while (start < str_set.len);
  return IntVal(0);
}

void StringFunctions::ParseUrlPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  if (!ctx->IsArgConstant(1)) return;
  DCHECK_EQ(ctx->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* part = reinterpret_cast<StringVal*>(ctx->GetConstantArg(1));
  if (part->is_null) return;
  UrlParser::UrlPart* url_part = new UrlParser::UrlPart;
  *url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(*part));
  if (*url_part == UrlParser::INVALID) {
    stringstream ss;
    ss << "Invalid URL part: " << AnyValUtil::ToString(*part) << endl
       << "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', "
       << "'USERINFO', and 'QUERY')";
    ctx->SetError(ss.str().c_str());
    return;
  }
  ctx->SetFunctionState(scope, url_part);
}

StringVal StringFunctions::ParseUrl(
    FunctionContext* ctx, const StringVal& url, const StringVal& part) {
  if (url.is_null || part.is_null) return StringVal::null();
  void* state = ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL);
  UrlParser::UrlPart url_part;
  if (state != NULL) {
    url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
  } else {
    DCHECK(!ctx->IsArgConstant(1));
    url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(part));
  }

  StringValue result;
  if (!UrlParser::ParseUrl(StringValue::FromStringVal(url), url_part, &result)) {
    // url is malformed, or url_part is invalid.
    if (url_part == UrlParser::INVALID) {
      stringstream ss;
      ss << "Invalid URL part: " << AnyValUtil::ToString(part);
      ctx->AddWarning(ss.str().c_str());
    } else {
      stringstream ss;
      ss << "Could not parse URL: " << AnyValUtil::ToString(url);
      ctx->AddWarning(ss.str().c_str());
    }
    return StringVal::null();
  }
  StringVal result_sv;
  result.ToStringVal(&result_sv);
  return result_sv;
}

void StringFunctions::ParseUrlClose(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  UrlParser::UrlPart* url_part =
      reinterpret_cast<UrlParser::UrlPart*>(ctx->GetFunctionState(scope));
  if (url_part == NULL) return;
  delete url_part;
}

StringVal StringFunctions::ParseUrlKey(FunctionContext* ctx, const StringVal& url,
                                       const StringVal& part, const StringVal& key) {
  if (url.is_null || part.is_null || key.is_null) return StringVal::null();
  void* state = ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL);
  UrlParser::UrlPart url_part;
  if (state != NULL) {
    url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
  } else {
    DCHECK(!ctx->IsArgConstant(1));
    url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(part));
  }

  StringValue result;
  if (!UrlParser::ParseUrlKey(StringValue::FromStringVal(url), url_part,
                              StringValue::FromStringVal(key), &result)) {
    // url is malformed, or url_part is invalid.
    if (url_part == UrlParser::INVALID) {
      stringstream ss;
      ss << "Invalid URL part: " << AnyValUtil::ToString(part);
      ctx->AddWarning(ss.str().c_str());
    } else {
      stringstream ss;
      ss << "Could not parse URL: " << AnyValUtil::ToString(url);
      ctx->AddWarning(ss.str().c_str());
    }
    return StringVal::null();
  }
  StringVal result_sv;
  result.ToStringVal(&result_sv);
  return result_sv;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

}
