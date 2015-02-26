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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
<<<<<<< HEAD

#include "exprs/timestamp-functions.h"
#include "exprs/expr.h"
=======
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/timestamp-functions.h"
#include "exprs/expr.h"
#include "exprs/anyval-util.h"

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/tuple-row.h"
#include "runtime/timestamp-value.h"
#include "util/path-builder.h"
#include "runtime/string-value.inline.h"
<<<<<<< HEAD
=======
#include "udf/udf.h"
#include "udf/udf-internal.h"
#include "runtime/runtime-state.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

#define TIMEZONE_DATABASE "be/files/date_time_zonespec.csv"

using namespace boost;
using namespace boost::posix_time;
using namespace boost::local_time;
using namespace boost::gregorian;
<<<<<<< HEAD
using namespace std;

namespace impala {

local_time::tz_database TimezoneDatabase::tz_database_;
vector<string> TimezoneDatabase::tz_region_list_;

void* TimestampFunctions::FromUnix(Expr* e, TupleRow* row) {
  DCHECK_LE(e->GetNumChildren(), 2);
  DCHECK_NE(e->GetNumChildren(), 0);
  
  Expr* op = e->children()[0];
  uint32_t* intp = reinterpret_cast<uint32_t*>(op->GetValue(row));
  if (intp == NULL) return NULL;
  TimestampValue t(boost::posix_time::from_time_t(*intp));

  // If there is a second argument then it's a format statement.
  // Otherwise the string is in the default format.
  if (e->GetNumChildren() == 2) {
    Expr* fmtop = e->children()[1];
    StringValue* format = reinterpret_cast<StringValue*>(fmtop->GetValue(row));
    if (CheckFormat(format) == NULL) return NULL;
    if (format->len == 10) {
      // If the format is yyyy-MM-dd then set the time to invalid.
      t.set_time(time_duration(not_a_date_time));
    } 
  }

  e->result_.SetStringVal(lexical_cast<string>(t));
  return &e->result_.string_val;
}

void* TimestampFunctions::Unix(Expr* e, TupleRow* row) {
  DCHECK_LE(e->GetNumChildren(), 2);
  TimestampValue* tv;
  if (e->GetNumChildren() == 0) {
    // Expr::Prepare put the current timestamp here.
    tv = &e->result_.timestamp_val;
  } else if (e->GetNumChildren() == 1) {
    Expr* op = e->children()[0];
    tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
    if (tv == NULL) return NULL;
  } else {
    Expr* op = e->children()[0];
    StringValue* value = reinterpret_cast<StringValue*>(op->GetValue(row));
    Expr* fmtop = e->children()[1];
    StringValue* format = reinterpret_cast<StringValue*>(fmtop->GetValue(row));

    if (value == NULL || format == NULL || CheckFormat(format) == NULL) return NULL;

    // Trim the value of blank space to be more user friendly.
    StringValue tvalue = value->Trim();

    // Check to see that the string roughly matches the format.  TimestampValue
    // will kick out bad internal format but will accept things that don't 
    // match what we have here.
    if (format->len != tvalue.len) {
      string fmt(format->ptr, format->len);
      string str(tvalue.ptr, tvalue.len);
      LOG(WARNING) << "Timestamp: " << str << " does not match format: " << fmt;
      return NULL;
    }

    TimestampValue val(tvalue.ptr, tvalue.len);
    tv = &val;
    if (tv->date().is_special()) return NULL;
  }

  ptime temp;
  tv->ToPtime(&temp);
  e->result_.int_val = static_cast<int32_t>(to_time_t(temp));
  return &e->result_.int_val;
}

// TODO: accept Java data/time format strings:
// http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
// Convert them to boost format strings.
StringValue* TimestampFunctions::CheckFormat(StringValue* format) {
  // For now the format  must be of the form: yyyy-MM-dd HH:mm:ss
  // where the time part is optional.
  switch(format->len) {
    case 10:
      if (strncmp(format->ptr, "yyyy-MM-dd", 10) == 0) return format;
      break;
    case 19:
      if (strncmp(format->ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) return format;
      break;
    default: 
      break;
  }
  ReportBadFormat(format);
  return NULL;
}

void TimestampFunctions::ReportBadFormat(StringValue* format) {
  string format_str(format->ptr, format->len);
  LOG(WARNING) << "Bad date/time conversion format: " << format_str 
               << " Format must be: 'yyyy-MM-dd[ HH:mm:ss]'";
}

void* TimestampFunctions::Year(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  // If the value has been set to not_a_date_time then it will be marked special
  // therefore there is no valid date component and this function returns NULL.
  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().year();
  return &e->result_.int_val;
}

void* TimestampFunctions::Month(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().month();
  return &e->result_.int_val;
}

void* TimestampFunctions::Day(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().day_of_year();
  return &e->result_.int_val;
}

void* TimestampFunctions::DayOfMonth(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().day();
  return &e->result_.int_val;
}

void* TimestampFunctions::WeekOfYear(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().week_number();
  return &e->result_.int_val;
}

void* TimestampFunctions::Hour(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().hours();
  return &e->result_.int_val;
}

void* TimestampFunctions::Minute(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().minutes();
  return &e->result_.int_val;
}

void* TimestampFunctions::Second(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().seconds();
  return &e->result_.int_val;
}

void* TimestampFunctions::Now(Expr* e, TupleRow* row) {
  // Make sure FunctionCall::Prepare() properly set the timestamp value.
  DCHECK(!e->result_.timestamp_val.date().is_special());
  return &e->result_.timestamp_val;
}

void* TimestampFunctions::ToDate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  string result = to_iso_extended_string(tv->date());
  e->result_.SetStringVal(result);
  return &e->result_.string_val;
}

void* TimestampFunctions::YearsAdd(Expr* e, TupleRow* row) {
  return TimestampDateOp<years>(e, row, true);
}

void* TimestampFunctions::YearsSub(Expr* e, TupleRow* row) {
  return TimestampDateOp<years>(e, row, false);
}

void* TimestampFunctions::MonthsAdd(Expr* e, TupleRow* row) {
  return TimestampDateOp<months>(e, row, true);
}

void* TimestampFunctions::MonthsSub(Expr* e, TupleRow* row) {
  return TimestampDateOp<months>(e, row, false);
}

void* TimestampFunctions::WeeksAdd(Expr* e, TupleRow* row) {
  return TimestampDateOp<weeks>(e, row, true);
}

void* TimestampFunctions::WeeksSub(Expr* e, TupleRow* row) {
  return TimestampDateOp<weeks>(e, row, false);
}

void* TimestampFunctions::DaysAdd(Expr* e, TupleRow* row) {
  return TimestampDateOp<date_duration>(e, row, true);
}

void* TimestampFunctions::DaysSub(Expr* e, TupleRow* row) {
  return TimestampDateOp<date_duration>(e, row, false);
}

void* TimestampFunctions::HoursAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<hours>(e, row, true);
}

void* TimestampFunctions::HoursSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<hours>(e, row, false);
}

void* TimestampFunctions::MinutesAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<minutes>(e, row, true);
}

void* TimestampFunctions::MinutesSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<minutes>(e, row, false);
}

void* TimestampFunctions::SecondsAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<seconds>(e, row, true);
}

void* TimestampFunctions::SecondsSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<seconds>(e, row, false);
}

void* TimestampFunctions::MillisAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<milliseconds>(e, row, true);
}

void* TimestampFunctions::MillisSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<milliseconds>(e, row, false);
}

void* TimestampFunctions::MicrosAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<microseconds>(e, row, true);
}

void* TimestampFunctions::MicrosSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<microseconds>(e, row, false);
}

void* TimestampFunctions::NanosAdd(Expr* e, TupleRow* row) {
  return TimestampTimeOp<nanoseconds>(e, row, true);
}

void* TimestampFunctions::NanosSub(Expr* e, TupleRow* row) {
  return TimestampTimeOp<nanoseconds>(e, row, false);
}

template <class UNIT>
void* TimestampFunctions::TimestampDateOp(Expr* e, TupleRow* row, bool is_add) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  int32_t* count = reinterpret_cast<int32_t*>(op2->GetValue(row));
  if (tv == NULL || count == NULL) return NULL;

  if (tv->date().is_special()) return NULL;

  UNIT unit(*count);
  TimestampValue
      value((is_add ? tv->date() + unit : tv->date() - unit), tv->time_of_day());
  e->result_.timestamp_val = value;

  return &e->result_.timestamp_val;
}

template <class UNIT>
void* TimestampFunctions::TimestampTimeOp(Expr* e, TupleRow* row, bool is_add) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  int32_t* count = reinterpret_cast<int32_t*>(op2->GetValue(row));
  if (tv == NULL || count == NULL) return NULL;

  if (tv->date().is_special()) return NULL;

  UNIT unit(*count);
  ptime p(tv->date(), tv->time_of_day());
  TimestampValue value(is_add ? p + unit : p - unit);
  e->result_.timestamp_val = value;

  return &e->result_.timestamp_val;
}

void* TimestampFunctions::DateDiff(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv1 = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  TimestampValue* tv2 = reinterpret_cast<TimestampValue*>(op2->GetValue(row));
  if (tv1 == NULL || tv2 == NULL) return NULL;

  if (tv1->date().is_special()) return NULL;
  if (tv2->date().is_special()) return NULL;

  e->result_.int_val = (tv2->date() - tv1->date()).days();
  return &e->result_.int_val;
}

void* TimestampFunctions::FromUtc(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  StringValue* tz = reinterpret_cast<StringValue*>(op2->GetValue(row));
  if (tv == NULL || tz == NULL) return NULL;

  if (tv->NotADateTime()) return NULL;

  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(tz->DebugString());
  // This should raise some sort of error or at least null. Hive just ignores it.
  if (timezone == NULL) {
    LOG(ERROR) << "Unknown timezone '" << *tz << "'" << endl;
    e->result_.timestamp_val = *tv;
    return &e->result_.timestamp_val;
  }
  ptime temp;
  tv->ToPtime(&temp);
  local_date_time lt(temp, timezone);
  e->result_.timestamp_val = lt.local_time();
  return &e->result_.timestamp_val;
}

void* TimestampFunctions::ToUtc(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  StringValue* tz = reinterpret_cast<StringValue*>(op2->GetValue(row));
  if (tv == NULL || tz == NULL) return NULL;

  if (tv->NotADateTime()) return NULL;

  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(tz->DebugString());
  // This should raise some sort of error or at least null. Hive just ignores it.
  if (timezone == NULL) {
    LOG(ERROR) << "Unknown timezone '" << *tz << "'" << endl;
    e->result_.timestamp_val = *tv;
    return &e->result_.timestamp_val;
  }
  local_date_time lt(tv->date(), tv->time_of_day(),
                     timezone, local_date_time::NOT_DATE_TIME_ON_ERROR);
  e->result_.timestamp_val = TimestampValue(lt.utc_time());
  return &e->result_.timestamp_val;
=======
using namespace impala_udf;
using namespace std;
using namespace strings;

namespace impala {

// Constant strings used for DayName function.
const char* TimestampFunctions::SUNDAY = "Sunday";
const char* TimestampFunctions::MONDAY = "Monday";
const char* TimestampFunctions::TUESDAY = "Tuesday";
const char* TimestampFunctions::WEDNESDAY = "Wednesday";
const char* TimestampFunctions::THURSDAY = "Thursday";
const char* TimestampFunctions::FRIDAY = "Friday";
const char* TimestampFunctions::SATURDAY = "Saturday";

void TimestampFunctions::UnixAndFromUnixPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  DateTimeFormatContext* dt_ctx = NULL;
  if (context->IsArgConstant(1)) {
    StringVal fmt_val = *reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    const StringValue& fmt_ref = StringValue::FromStringVal(fmt_val);
    if (fmt_val.is_null || fmt_ref.len <= 0) {
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
    dt_ctx = new DateTimeFormatContext(fmt_ref.ptr, fmt_ref.len);
    bool parse_result = TimestampParser::ParseFormatTokens(dt_ctx);
    if (!parse_result) {
      delete dt_ctx;
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
  } else {
    // If our format string is constant, then we benefit from it only being parsed once in
    // the code above. If it's not constant, then we can reuse a context by resetting it.
    // This is much cheaper vs alloc/dealloc'ing a context for each evaluation.
    dt_ctx = new DateTimeFormatContext();
  }
  context->SetFunctionState(scope, dt_ctx);
}

void TimestampFunctions::UnixAndFromUnixClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    DateTimeFormatContext* dt_ctx =
        reinterpret_cast<DateTimeFormatContext*>(context->GetFunctionState(scope));
    delete dt_ctx;
  }
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp) {
  if (intp.is_null) return StringVal::null();
  TimestampValue t(boost::posix_time::from_time_t(intp.val));
  return AnyValUtil::FromString(context, lexical_cast<string>(t));
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len <= 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return StringVal::null();
  }
  if (intp.is_null) return StringVal::null();

  TimestampValue t(boost::posix_time::from_time_t(intp.val));
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);
  if (!context->IsArgConstant(1)) {
    dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
    if (!TimestampParser::ParseFormatTokens(dt_ctx)){
      TimestampFunctions::ReportBadFormat(context, fmt, false);
      return StringVal::null();
    }
  }

  int buff_len = dt_ctx->fmt_out_len + 1;
  StringVal result(context, buff_len);
  result.len = t.Format(*dt_ctx, buff_len, reinterpret_cast<char*>(result.ptr));
  if (result.len <= 0) return StringVal::null();
  return result;
}

IntVal TimestampFunctions::Unix(FunctionContext* context, const StringVal& string_val,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len <= 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return IntVal::null();
  }
  if(string_val.is_null || string_val.len <= 0) return IntVal::null();

  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);

  if (!context->IsArgConstant(1)) {
     dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
     if (!TimestampParser::ParseFormatTokens(dt_ctx)){
       ReportBadFormat(context, fmt, false);
       return IntVal::null();
     }
  }

  TimestampValue default_tv;
  TimestampValue* tv = &default_tv;
  default_tv = TimestampValue(
      reinterpret_cast<const char*>(string_val.ptr), string_val.len, *dt_ctx);
  if (tv->date().is_special()) return IntVal::null();
  ptime temp;
  tv->ToPtime(&temp);
  return IntVal(to_time_t(temp));
}

IntVal TimestampFunctions::Unix(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value_ref = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value_ref.get_date().is_special()) return IntVal::null();
  ptime temp;
  ts_value_ref.ToPtime(&temp);
  return IntVal(to_time_t(temp));
}

IntVal TimestampFunctions::Unix(FunctionContext* context) {
  TimestampValue default_tv;
  TimestampValue* tv = &default_tv;
  default_tv = TimestampValue(context->impl()->state()->now());
  if (tv->date().is_special()) return IntVal::null();
  ptime temp;
  tv->ToPtime(&temp);
  return IntVal(to_time_t(temp));
}

IntVal TimestampFunctions::UnixFromString(FunctionContext* context, const StringVal& sv) {
  if (sv.is_null) return IntVal::null();
  TimestampValue tv(reinterpret_cast<const char *>(sv.ptr), sv.len);
  if (tv.date().is_special()) return IntVal::null();
  ptime temp;
  tv.ToPtime(&temp);
  return IntVal(to_time_t(temp));
}

void TimestampFunctions::ReportBadFormat(FunctionContext* context,
    const StringVal& format, bool is_error) {
  stringstream ss;
  const StringValue& fmt = StringValue::FromStringVal(format);
  if (format.is_null || format.len <= 0) {
    ss << "Bad date/time coversion format: format string is NULL or has 0 length";
  } else {
    ss << "Bad date/time coversion format: " << fmt.DebugString();
  }
  if (is_error) {
    context->SetError(ss.str().c_str());
  } else {
    context->AddWarning(ss.str().c_str());
  }
}

StringVal TimestampFunctions::DayName(FunctionContext* context, const TimestampVal& ts) {
  if (ts.is_null) return StringVal::null();
  IntVal dow = DayOfWeek(context, ts);
  switch(dow.val) {
    case 1: return StringVal(SUNDAY);
    case 2: return StringVal(MONDAY);
    case 3: return StringVal(TUESDAY);
    case 4: return StringVal(WEDNESDAY);
    case 5: return StringVal(THURSDAY);
    case 6: return StringVal(FRIDAY);
    case 7: return StringVal(SATURDAY);
    default: return StringVal::null();
   }
}

IntVal TimestampFunctions::Year(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return IntVal::null();
  return IntVal(ts_value.get_date().year());
}


IntVal TimestampFunctions::Month(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return IntVal::null();
  return IntVal(ts_value.get_date().month());
}


IntVal TimestampFunctions::DayOfWeek(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue ts_value_ref = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value_ref.get_date().is_special()) return IntVal::null();
  // Sql has the result in [1,7] where 1 = Sunday. Boost has 0 = Sunday.
  return IntVal(ts_value_ref.get_date().day_of_week() + 1);
}

IntVal TimestampFunctions::DayOfMonth(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return IntVal::null();
  return IntVal(ts_value.get_date().day());
}

IntVal TimestampFunctions::DayOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return IntVal::null();
  return IntVal(ts_value.get_date().day_of_year());
}

IntVal TimestampFunctions::WeekOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return IntVal::null();
  return IntVal(ts_value.get_date().week_number());
}

IntVal TimestampFunctions::Hour(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_time().is_special()) return IntVal::null();
  return IntVal(ts_value.get_time().hours());
}

IntVal TimestampFunctions::Minute(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_time().is_special()) return IntVal::null();
  return IntVal(ts_value.get_time().minutes());
}

IntVal TimestampFunctions::Second(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_time().is_special()) return IntVal::null();
  return IntVal(ts_value.get_time().seconds());
}

TimestampVal TimestampFunctions::Now(FunctionContext* context) {
  const TimestampValue* now = context->impl()->state()->now();
  if (now->NotADateTime()) return TimestampVal::null();
  TimestampVal return_val;
  now->ToTimestampVal(&return_val);
  return return_val;
}

StringVal TimestampFunctions::ToDate(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return StringVal::null();
  const TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  string result = to_iso_extended_string(ts_value.get_date());
  return AnyValUtil::FromString(context, result);
}

template <bool ISADD, class VALTYPE, class UNIT>
TimestampVal TimestampFunctions::DateAddSub(FunctionContext* context,
    const TimestampVal& ts_val, const VALTYPE& count) {
  if (ts_val.is_null || count.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);

  if (ts_value.get_date().is_special()) return TimestampVal::null();
  UNIT unit(count.val);
  TimestampValue value;
  try {
    // Adding/subtracting boost::gregorian::dates can throw (via constructing a new date)
    value = TimestampValue(
        (ISADD ? ts_value.get_date() + unit : ts_value.get_date() - unit),
        ts_value.get_time());
  } catch (const std::exception& e) {
    context->AddWarning(Substitute("Cannot $0 date interval $1: $2",
        ISADD ? "add" : "subtract", count.val, e.what()).c_str());
    return TimestampVal::null();
  }
  TimestampVal return_val;
  value.ToTimestampVal(&return_val);
  return return_val;
}

template <bool ISADD, class VALTYPE, class UNIT>
TimestampVal TimestampFunctions::TimeAddSub(FunctionContext * context,
    const TimestampVal& ts_val, const VALTYPE& count) {
  if (ts_val.is_null || count.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.get_date().is_special()) return TimestampVal::null();
  UNIT unit(count.val);
  ptime p(ts_value.get_date(), ts_value.get_time());
  TimestampValue value(ISADD ? p + unit : p - unit);
  TimestampVal return_val;
  value.ToTimestampVal(&return_val);
  return return_val;
}

IntVal TimestampFunctions::DateDiff(FunctionContext* context,
    const TimestampVal& ts_val1,
    const TimestampVal& ts_val2) {
  if (ts_val1.is_null || ts_val2.is_null) return IntVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts_val1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts_val2);
  if (ts_value1.get_date().is_special() || ts_value2.get_date().is_special()) {
    return IntVal::null();
  }
  return IntVal((ts_value1.get_date() - ts_value2.get_date()).days());
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.NotADateTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
  if (timezone == NULL) {
    // This should return null. Hive just ignores it.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  ptime temp;
  ts_value.ToPtime(&temp);
  local_date_time lt(temp, timezone);
  TimestampValue return_value = lt.local_time();
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::ToUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (ts_value.NotADateTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
  // This should raise some sort of error or at least null. Hive Just ignores it.
  if (timezone == NULL) {
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  local_date_time lt(ts_value.get_date(), ts_value.get_time(),
      timezone, local_date_time::NOT_DATE_TIME_ON_ERROR);
  TimestampValue return_value(lt.utc_time());
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

TimezoneDatabase::TimezoneDatabase() {
  // Create a temporary file and write the timezone information.  The boost
  // interface only loads this format from a file.  We don't want to raise
  // an error here since this is done when the backend is created and this
  // information might not actually get used by any queries.
  char filestr[] = "/tmp/impala.tzdb.XXXXXXX";
  FILE* file;
  int fd;
  if ((fd = mkstemp(filestr)) == -1) {
    LOG(ERROR) << "Could not create temporary timezone file: " << filestr;
    return;
  }
  if ((file = fopen(filestr, "w")) == NULL) {
    unlink(filestr);
    close(fd);
    LOG(ERROR) << "Could not open temporary timezone file: " << filestr;
    return;
  }
  if (fputs(TIMEZONE_DATABASE_STR, file) == EOF) {
    unlink(filestr);
    close(fd);
    fclose(file);
    LOG(ERROR) << "Could not load temporary timezone file: " << filestr;
    return;
  }
  fclose(file);
  tz_database_.load_from_file(string(filestr));
  tz_region_list_ = tz_database_.region_list();
  unlink(filestr);
  close(fd);
}

TimezoneDatabase::~TimezoneDatabase() { }

<<<<<<< HEAD
time_zone_ptr TimezoneDatabase::FindTimezone(const string& tz) {
  // See if they specified a zone id
  if (tz.find_first_of('/') != string::npos)
    return  tz_database_.time_zone_from_region(tz);
=======
time_zone_ptr TimezoneDatabase::FindTimezone(const string& tz, const TimestampValue& tv) {
  // The backing database does not capture some subtleties, there are special cases
  if ((tv.get_date().year() > 2011
       || (tv.get_date().year() == 2011 && tv.get_date().month() >= 4))
      && (iequals("Europe/Moscow", tz) || iequals("Moscow", tz) || iequals("MSK", tz))) {
    // We transition in April 2011 from using the tz_database_ to a custom rule
    // Russia stopped using daylight savings in 2011, the tz_database_ is
    // set up assuming Russia uses daylight saving every year.
    // Sun, Mar 27, 2:00AM Moscow clocks moved forward +1 hour (a total of GMT +4)
    // Specifically,
    // UTC Time 26 Mar 2011 22:59:59 +0000 ===> Sun Mar 27 01:59:59 MSK 2011
    // UTC Time 26 Mar 2011 23:00:00 +0000 ===> Sun Mar 27 03:00:00 MSK 2011
    // This means in 2011, The database rule will apply DST starting March 26 2011.
    // This will be a correct +4 offset, and the database rule can apply until
    // Oct 31 when tz_database_ will incorrectly attempt to turn clocks backwards 1 hour.
    return TIMEZONE_MSK_2011_NODST;
  }

  // See if they specified a zone id
  if (tz.find_first_of('/') != string::npos) {
    return tz_database_.time_zone_from_region(tz);
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  for (vector<string>::const_iterator iter = tz_region_list_.begin();
       iter != tz_region_list_.end(); ++iter) {
    time_zone_ptr tzp = tz_database_.time_zone_from_region(*iter);
    DCHECK(tzp != NULL);
    if (tzp->dst_zone_abbrev() == tz)
      return tzp;
    if (tzp->std_zone_abbrev() == tz)
      return tzp;
    if (tzp->dst_zone_name() == tz)
      return tzp;
    if (tzp->std_zone_name() == tz)
      return tzp;
  }
  return time_zone_ptr();
<<<<<<< HEAD

}

=======
}

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context, const IntVal& intp, const
  StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp,
    const StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context , const IntVal& intp);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp);

template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);

template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
