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

package com.cloudera.impala.analysis;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.impala.analysis.ArithmeticExpr.Operator;
<<<<<<< HEAD
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOpcode;
=======
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.base.Preconditions;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmeticExpr extends Expr {

  // Time units supported in timestamp arithmetic.
  public static enum TimeUnit {
    YEAR("YEAR"),
    MONTH("MONTH"),
    WEEK("WEEK"),
    DAY("DAY"),
    HOUR("HOUR"),
    MINUTE("MINUTE"),
    SECOND("SECOND"),
    MILLISECOND("MILLISECOND"),
    MICROSECOND("MICROSECOND"),
    NANOSECOND("NANOSECOND");

<<<<<<< HEAD
    private final String description;

    private TimeUnit(String description) {
      this.description = description;
=======
    private final String description_;

    private TimeUnit(String description) {
      this.description_ = description;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }

    @Override
    public String toString() {
<<<<<<< HEAD
      return description;
=======
      return description_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }

  private static Map<String, TimeUnit> TIME_UNITS_MAP = new HashMap<String, TimeUnit>();
  static {
    for (TimeUnit timeUnit : TimeUnit.values()) {
      TIME_UNITS_MAP.put(timeUnit.toString(), timeUnit);
      TIME_UNITS_MAP.put(timeUnit.toString() + "S", timeUnit);
    }
  }

  // Set for function call-like arithmetic.
<<<<<<< HEAD
  private final String funcName;
  private ArithmeticExpr.Operator op;

  // Keep the original string passed in the c'tor to resolve
  // ambiguities with other uses of IDENT during query parsing.
  private final String timeUnitIdent;
  private TimeUnit timeUnit;

  // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
  private final boolean intervalFirst;
=======
  private final String funcName_;
  private ArithmeticExpr.Operator op_;

  // Keep the original string passed in the c'tor to resolve
  // ambiguities with other uses of IDENT during query parsing.
  private final String timeUnitIdent_;
  private TimeUnit timeUnit_;

  // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
  private final boolean intervalFirst_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
  public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2,
      String timeUnitIdent) {
<<<<<<< HEAD
    this.funcName = funcName;
    this.timeUnitIdent = timeUnitIdent;
    this.intervalFirst = false;
    children.add(e1);
    children.add(e2);
=======
    this.funcName_ = funcName.toLowerCase();
    this.timeUnitIdent_ = timeUnitIdent;
    this.intervalFirst_ = false;
    children_.add(e1);
    children_.add(e2);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  // C'tor for non-function-call like arithmetic, e.g., 'a + interval b year'.
  // e1 always refers to the timestamp to be added/subtracted from, and e2
  // to the time value (even in the interval-first case).
  public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
      String timeUnitIdent, boolean intervalFirst) {
    Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
<<<<<<< HEAD
    this.funcName = null;
    this.op = op;
    this.timeUnitIdent = timeUnitIdent;
    this.intervalFirst = intervalFirst;
    children.add(e1);
    children.add(e2);
=======
    this.funcName_ = null;
    this.op_ = op;
    this.timeUnitIdent_ = timeUnitIdent;
    this.intervalFirst_ = intervalFirst;
    children_.add(e1);
    children_.add(e2);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected TimestampArithmeticExpr(TimestampArithmeticExpr other) {
    super(other);
    funcName_ = other.funcName_;
    op_ = other.op_;
    timeUnitIdent_ = other.timeUnitIdent_;
    timeUnit_ = other.timeUnit_;
    intervalFirst_ = other.intervalFirst_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
<<<<<<< HEAD
    super.analyze(analyzer);

    // Check if name of function call is date_sub or date_add.
    if (funcName != null) {
      if (funcName.toUpperCase().equals("DATE_ADD")) {
        op = ArithmeticExpr.Operator.ADD;
      } else if (funcName.toUpperCase().equals("DATE_SUB")) {
        op = ArithmeticExpr.Operator.SUBTRACT;
      } else {
        throw new AnalysisException("Encountered function name '" + funcName +
=======
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    if (funcName_ != null) {
      // Set op based on funcName for function-call like version.
      if (funcName_.equals("date_add")) {
        op_ = ArithmeticExpr.Operator.ADD;
      } else if (funcName_.equals("date_sub")) {
        op_ = ArithmeticExpr.Operator.SUBTRACT;
      } else {
        throw new AnalysisException("Encountered function name '" + funcName_ +
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
            "' in timestamp arithmetic expression '" + toSql() + "'. " +
            "Expected function name 'DATE_ADD' or 'DATE_SUB'.");
      }
    }
<<<<<<< HEAD
    timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase());
    if (timeUnit == null) {
      throw new AnalysisException("Invalid time unit '" + timeUnitIdent +
          "' in timestamp arithmetic expression '" + toSql() + "'.");
    }

    // The first child must return a timestamp.
    if (getChild(0).getType() != PrimitiveType.TIMESTAMP) {
      throw new AnalysisException("Operand '" + getChild(0).toSql() +
          "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
          getChild(0).getType() + "'. Expected type 'TIMESTAMP'.");
    }

    // The second child must be of type 'INT' or castable to it.
    if (getChild(1).getType() != PrimitiveType.INT) {
      if (PrimitiveType.isImplicitlyCastable(getChild(1).getType(), PrimitiveType.INT)) {
        castChild(PrimitiveType.INT, 1);
      } else {
        throw new AnalysisException("Operand '" + getChild(1).toSql() +
            "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
            getChild(1).getType() + "' which is incompatible with expected type 'INT'.");
      }
    }

    type = PrimitiveType.TIMESTAMP;
    opcode = getOpCode();
=======

    timeUnit_ = TIME_UNITS_MAP.get(timeUnitIdent_.toUpperCase());
    if (timeUnit_ == null) {
      throw new AnalysisException("Invalid time unit '" + timeUnitIdent_ +
          "' in timestamp arithmetic expression '" + toSql() + "'.");
    }

    // The first child must return a timestamp or null.
    if (!getChild(0).getType().isTimestamp() && !getChild(0).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(0).toSql() +
          "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
          getChild(0).getType().toSql() + "'. Expected type 'TIMESTAMP'.");
    }

    // The second child must be an integer type.
    if (!getChild(1).getType().isIntegerType() &&
        !getChild(1).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(1).toSql() +
          "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
          getChild(1).getType().toSql() + "'. Expected an integer type.");
    }

    String funcOpName = String.format("%sS_%s", timeUnit_.toString(),
        (op_ == ArithmeticExpr.Operator.ADD) ? "ADD" : "SUB");

    fn_ = getBuiltinFunction(analyzer, funcOpName.toLowerCase(),
         collectChildReturnTypes(), CompareMode.IS_SUPERTYPE_OF);
    castForFunctionCall(false);

    Preconditions.checkNotNull(fn_);
    Preconditions.checkState(fn_.getReturnType().isTimestamp());
    type_ = fn_.getReturnType();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
<<<<<<< HEAD
    msg.setOpcode(opcode);
  }

  public String getTimeUnitIdent() {
    return timeUnitIdent;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public ArithmeticExpr.Operator getOp() {
    return op;
  }

  private TExprOpcode getOpCode() {
    // Select appropriate opcode based on op and timeUnit.
    switch (timeUnit) {
      case YEAR: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_YEARS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_YEARS_SUB;
        }
      }
      case MONTH: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_MONTHS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_MONTHS_SUB;
        }
      }
      case WEEK: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_WEEKS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_WEEKS_SUB;
        }
      }
      case DAY: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_DAYS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_DAYS_SUB;
        }
      }
      case HOUR: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_HOURS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_HOURS_SUB;
        }
      }
      case MINUTE: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_MINUTES_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_MINUTES_SUB;
        }
      }
      case SECOND: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_SECONDS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_SECONDS_SUB;
        }
      }
      case MILLISECOND: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_MILLISECONDS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_MILLISECONDS_SUB;
        }
      }
      case MICROSECOND: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_MICROSECONDS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_MICROSECONDS_SUB;
        }
      }
      case NANOSECOND: {
        if (op == Operator.ADD) {
          return TExprOpcode.TIMESTAMP_NANOSECONDS_ADD;
        } else {
          return TExprOpcode.TIMESTAMP_NANOSECONDS_SUB;
        }
      }
      default: {
        Preconditions.checkState(false, "Unexpected time unit '" + timeUnit + "'.");
      }
    }
    return null;
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
    if (funcName != null) {
      // Function-call like version.
      strBuilder.append(funcName + "(");
      strBuilder.append(getChild(0).toSql() + ", ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql());
      strBuilder.append(" " + timeUnitIdent);
      strBuilder.append(")");
      return strBuilder.toString();
    }
    if (intervalFirst) {
      // Non-function-call like version with interval as first operand.
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent);
      strBuilder.append(" " + op.toString() + " ");
=======
  }

  public String getTimeUnitIdent() { return timeUnitIdent_; }
  public TimeUnit getTimeUnit() { return timeUnit_; }
  public ArithmeticExpr.Operator getOp() { return op_; }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    if (funcName_ != null) {
      // Function-call like version.
      strBuilder.append(funcName_.toUpperCase() + "(");
      strBuilder.append(getChild(0).toSql() + ", ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql());
      strBuilder.append(" " + timeUnitIdent_);
      strBuilder.append(")");
      return strBuilder.toString();
    }
    if (intervalFirst_) {
      // Non-function-call like version with interval as first operand.
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent_);
      strBuilder.append(" " + op_.toString() + " ");
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      strBuilder.append(getChild(0).toSql());
    } else {
      // Non-function-call like version with interval as second operand.
      strBuilder.append(getChild(0).toSql());
<<<<<<< HEAD
      strBuilder.append(" " + op.toString() + " ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent);
    }
    return strBuilder.toString();
  }
=======
      strBuilder.append(" " + op_.toString() + " ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent_);
    }
    return strBuilder.toString();
  }

  @Override
  public Expr clone() { return new TimestampArithmeticExpr(this); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
