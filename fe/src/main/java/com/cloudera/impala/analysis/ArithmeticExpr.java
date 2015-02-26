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

<<<<<<< HEAD
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
=======
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
<<<<<<< HEAD

public class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY("*", FunctionOperator.MULTIPLY),
    DIVIDE("/", FunctionOperator.DIVIDE),
    MOD("%", FunctionOperator.MOD),
    INT_DIVIDE("DIV", FunctionOperator.INT_DIVIDE),
    ADD("+", FunctionOperator.ADD),
    SUBTRACT("-", FunctionOperator.SUBTRACT),
    BITAND("&", FunctionOperator.BITAND),
    BITOR("|", FunctionOperator.BITOR),
    BITXOR("^", FunctionOperator.BITXOR),
    BITNOT("~", FunctionOperator.BITNOT);

    private final String description;
    private final FunctionOperator functionOp;

    private Operator(String description, FunctionOperator thriftOp) {
      this.description = description;
      this.functionOp = thriftOp;
    }

    @Override
    public String toString() {
      return description;
    }

    public FunctionOperator toFunctionOp() {
      return functionOp;
    }
  }

  private final Operator op;

  public Operator getOp() {
    return op;
  }

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkArgument(op == Operator.BITNOT && e2 == null
        || op != Operator.BITNOT && e2 != null);
    if (e2 != null) {
      children.add(e2);
    }
=======
import com.google.common.collect.Lists;

public class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY("*", "multiply"),
    DIVIDE("/", "divide"),
    MOD("%", "mod"),
    INT_DIVIDE("DIV", "int_divide"),
    ADD("+", "add"),
    SUBTRACT("-", "subtract"),
    BITAND("&", "bitand"),
    BITOR("|", "bitor"),
    BITXOR("^", "bitxor"),
    BITNOT("~", "bitnot");

    private final String description_;
    private final String name_;

    private Operator(String description, String name) {
      this.description_ = description;
      this.name_ = name;
    }

    @Override
    public String toString() { return description_; }
    public String getName() { return name_; }
  }

  private final Operator op_;

  public Operator getOp() { return op_; }

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument(op == Operator.BITNOT && e2 == null
        || op != Operator.BITNOT && e2 != null);
    if (e2 != null) children_.add(e2);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected ArithmeticExpr(ArithmeticExpr other) {
    super(other);
    op_ = other.op_;
  }

  public static void initBuiltins(Db db) {
    for (Type t: Type.getNumericTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.MULTIPLY.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.ADD.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.SUBTRACT.getName(), Lists.newArrayList(t, t), t));
    }
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
        Type.DOUBLE));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.DIVIDE.getName(),
        Lists.<Type>newArrayList(Type.DECIMAL, Type.DECIMAL),
        Type.DECIMAL));

    for (Type t: Type.getIntegerTypes()) {
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.MOD.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITAND.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITOR.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITXOR.getName(), Lists.newArrayList(t, t), t));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.BITNOT.getName(), Lists.newArrayList(t), t));
    }
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.MOD.getName(), Lists.<Type>newArrayList(
            Type.DECIMAL, Type.DECIMAL), Type.DECIMAL));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
<<<<<<< HEAD
        .add("op", op)
=======
        .add("op", op_)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        .addValue(super.debugString())
        .toString();
  }

  @Override
<<<<<<< HEAD
  public String toSql() {
    if (children.size() == 1) {
      return op.toString() + " " + getChild(0).toSql();
    } else {
      Preconditions.checkState(children.size() == 2);
      return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
=======
  public String toSqlImpl() {
    if (children_.size() == 1) {
      return op_.toString() + getChild(0).toSql();
    } else {
      Preconditions.checkState(children_.size() == 2);
      return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
<<<<<<< HEAD
    msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
    msg.setOpcode(opcode);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((ArithmeticExpr) obj).opcode == opcode;
=======
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  /**
   * Inserts a cast from child[childIdx] to targetType if one is necessary.
   * Note this is different from Expr.castChild() since arithmetic for decimals
   * the cast is handled as part of the operator and in general, the return type
   * does not match the input types.
   */
  void castChild(int childIdx, Type targetType) throws AnalysisException {
    Type t = getChild(childIdx).getType();
    if (t.matchesType(targetType)) return;
    if (targetType.isDecimal() && !t.isNull()) {
      Preconditions.checkState(t.isScalarType());
      targetType = ((ScalarType) t).getMinResolutionDecimal();
    }
    castChild(targetType, childIdx);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
<<<<<<< HEAD
    super.analyze(analyzer);
    for (Expr child: children) {
      Expr operand = (Expr) child;
      if (!operand.type.isNumericType()) {
        throw new AnalysisException("Arithmetic operation requires " +
            "numeric operands: " + toSql());
      }
    }

    // bitnot is the only unary op, deal with it here
    if (op == Operator.BITNOT) {
      type = getChild(0).getType();
      OpcodeRegistry.Signature match =
        OpcodeRegistry.instance().getFunctionInfo(op.functionOp, type);
      if (match == null) {
        throw new AnalysisException("Bitwise operations only allowed on fixed-point types: "
            + toSql());
      }
      Preconditions.checkState(type == match.returnType);
      opcode = match.opcode;
      return;
    }

    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();  // only bitnot is unary

    switch (op) {
      case MULTIPLY:
      case ADD:
      case SUBTRACT:
        // numeric ops must be promoted to highest-resolution type
        // (otherwise we can't guarantee that a <op> b won't result in an overflow/underflow)
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2).getMaxResolutionType();
        Preconditions.checkState(type.isValid());
        break;
      case MOD:
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        break;
      case DIVIDE:
        type = PrimitiveType.DOUBLE;
        break;
=======
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    for (Expr child: children_) {
      Expr operand = (Expr) child;
      if (!operand.type_.isNumericType() && !operand.type_.isNull()) {
        String errMsg = "Arithmetic operation requires numeric operands: " + toSql();
        if (operand instanceof Subquery && !operand.type_.isScalarType()) {
          errMsg = "Subquery must return a single row: " + operand.toSql();
        }
        throw new AnalysisException(errMsg);
      }
    }

    Type t0 = getChild(0).getType();
    // bitnot is the only unary op, deal with it here
    if (op_ == Operator.BITNOT) {
      // Special case ~NULL to resolve to TYPE_INT.
      if (!t0.isNull() && !t0.isIntegerType()) {
        throw new AnalysisException("Bitwise operations only allowed on integer " +
            "types: " + toSql());
      }
      if (t0.isNull()) castChild(0, Type.INT);
      fn_ = getBuiltinFunction(analyzer, op_.getName(), collectChildReturnTypes(),
          CompareMode.IS_SUPERTYPE_OF);
      Preconditions.checkNotNull(fn_);
      castForFunctionCall(false);
      type_ = fn_.getReturnType();
      return;
    }

    Preconditions.checkState(children_.size() == 2); // only bitnot is unary
    convertNumericLiteralsFromDecimal(analyzer);
    t0 = getChild(0).getType();
    Type t1 = getChild(1).getType();

    String fnName = op_.getName();
    switch (op_) {
      case ADD:
      case SUBTRACT:
      case DIVIDE:
      case MULTIPLY:
      case MOD:
        type_ = TypesUtil.getArithmeticResultType(t0, t1, op_);
        // If both of the children are null, we'll default to the DOUBLE version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = Type.DOUBLE;
        break;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      case INT_DIVIDE:
      case BITAND:
      case BITOR:
      case BITXOR:
<<<<<<< HEAD
        if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
          throw new AnalysisException(
              "Invalid floating point argument to operation " +
              op.toString() + ": " + this.toSql());
        }
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        // the result is always an integer
        Preconditions.checkState(type.isFixedPointType());
        break;
      default:
        // the programmer forgot to deal with a case
        Preconditions.checkState(false,
            "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
        break;
    }

    type = castBinaryOp(type);
    OpcodeRegistry.Signature match =
      OpcodeRegistry.instance().getFunctionInfo(op.toFunctionOp(), type, type);
    this.opcode = match.opcode;
  }
=======
        if ((!t0.isNull() & !t0.isIntegerType()) ||
            (!t1.isNull() && !t1.isIntegerType())) {
          throw new AnalysisException("Invalid non-integer argument to operation '" +
              op_.toString() + "': " + this.toSql());
        }
        type_ = Type.getAssignmentCompatibleType(t0, t1);
        // If both of the children are null, we'll default to the INT version of the
        // operator. This prevents the BE from seeing NULL_TYPE.
        if (type_.isNull()) type_ = Type.INT;
        Preconditions.checkState(type_.isIntegerType());
        break;

      default:
        // the programmer forgot to deal with a case
        Preconditions.checkState(false,
            "Unknown arithmetic operation " + op_.toString() + " in: " + this.toSql());
        break;
    }

    // Don't cast from decimal to decimal. The BE function can just handle this.
    if (!(type_.isDecimal() && t0.isDecimal())) castChild(0, type_);
    if (!(type_.isDecimal() && t1.isDecimal())) castChild(1, type_);
    t0 = getChild(0).getType();
    t1 = getChild(1).getType();

    // Use MATH_MOD function operator for floating-point modulo.
    // TODO remove this when we have operators implemented using the UDF interface
    // and we can resolve this just using function overloading.
    if ((t0.isFloatingPointType() || t1.isFloatingPointType()) &&
        op_ == ArithmeticExpr.Operator.MOD) {
      fnName = "fmod";
    }

    fn_ = getBuiltinFunction(analyzer, fnName, collectChildReturnTypes(),
        CompareMode.IS_IDENTICAL);
    if (fn_ == null) {
      Preconditions.checkState(false, String.format("No match " +
          "for '%s' with operand types %s and %s", toSql(), t0, t1));
    }
    Preconditions.checkState(type_.matchesType(fn_.getReturnType()));
  }

  @Override
  public Expr clone() { return new ArithmeticExpr(this); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
