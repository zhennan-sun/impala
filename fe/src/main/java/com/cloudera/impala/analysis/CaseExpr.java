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

import java.util.List;

<<<<<<< HEAD
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TCaseExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * CaseExpr represents the SQL expression
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr).
 * If a case expr is given then it is the first child.
 * If an else expr is given then it is the last child.
 *
 */
public class CaseExpr extends Expr {
  private boolean hasCaseExpr;
  private boolean hasElseExpr;
=======
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCaseExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * CASE and DECODE are represented using this class. The backend implementation is
 * always the "case" function.
 *
 * The internal representation of
 *   CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr). If a case
 * expr is given then it is the first child. If an else expr is given then it is the
 * last child.
 *
 * The internal representation of
 *   DECODE(expr, key_expr, val_expr [, key_expr, val_expr ...] [, default_val_expr])
 * has a pair of children for each pair of key/val_expr and an additional child if the
 * default_val_expr was given. The first child represents the comparison of expr to
 * key_expr. Decode has three forms:
 *   1) DECODE(expr, null_literal, val_expr) -
 *       child[0] = IsNull(expr)
 *   2) DECODE(expr, non_null_literal, val_expr) -
 *       child[0] = Eq(expr, literal)
 *   3) DECODE(expr1, expr2, val_expr) -
 *       child[0] = Or(And(IsNull(expr1), IsNull(expr2)),  Eq(expr1, expr2))
 * The children representing val_expr (child[1]) and default_val_expr (child[2]) are
 * simply the exprs themselves.
 *
 * Example of equivalent CASE for DECODE(foo, 'bar', 1, col, 2, NULL, 3, 4):
 *   CASE
 *     WHEN foo = 'bar' THEN 1   -- no need for IS NULL check
 *     WHEN foo IS NULL AND col IS NULL OR foo = col THEN 2
 *     WHEN foo IS NULL THEN 3  -- no need for equality check
 *     ELSE 4
 *   END
 */
public class CaseExpr extends Expr {

  // Set if constructed from a DECODE, null otherwise.
  private FunctionCallExpr decodeExpr_;

  private boolean hasCaseExpr_;
  private boolean hasElseExpr_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  public CaseExpr(Expr caseExpr, List<CaseWhenClause> whenClauses, Expr elseExpr) {
    super();
    if (caseExpr != null) {
<<<<<<< HEAD
      children.add(caseExpr);
      hasCaseExpr = true;
    }
    for (CaseWhenClause whenClause: whenClauses) {
      Preconditions.checkNotNull(whenClause.getWhenExpr());
      children.add(whenClause.getWhenExpr());
      Preconditions.checkNotNull(whenClause.getThenExpr());
      children.add(whenClause.getThenExpr());
    }
    if (elseExpr != null) {
      children.add(elseExpr);
      hasElseExpr = true;
=======
      children_.add(caseExpr);
      hasCaseExpr_ = true;
    }
    for (CaseWhenClause whenClause: whenClauses) {
      Preconditions.checkNotNull(whenClause.getWhenExpr());
      children_.add(whenClause.getWhenExpr());
      Preconditions.checkNotNull(whenClause.getThenExpr());
      children_.add(whenClause.getThenExpr());
    }
    if (elseExpr != null) {
      children_.add(elseExpr);
      hasElseExpr_ = true;
    }
  }

  /**
   * Constructs an equivalent CaseExpr representation.
   *
   * The DECODE behavior is basically the same as the hasCaseExpr_ version of CASE.
   * Though there is one difference. NULLs are considered equal when comparing the
   * argument to be decoded with the candidates. This differences is for compatibility
   * with Oracle. http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions040.htm.
   * To account for the difference, the CASE representation will use the non-hasCaseExpr_
   * version.
   *
   * The return type of DECODE differs from that of Oracle when the third argument is
   * the NULL literal. In Oracle the return type is STRING. In Impala the return type is
   * determined by the implicit casting rules (i.e. it's not necessarily a STRING). This
   * is done so seemingly normal usages such as DECODE(int_col, tinyint_col, NULL,
   * bigint_col) will avoid type check errors (STRING incompatible with BIGINT).
   */
  public CaseExpr(FunctionCallExpr decodeExpr) {
    super();
    decodeExpr_ = decodeExpr;
    hasCaseExpr_ = false;

    int childIdx = 0;
    Expr encoded = null;
    Expr encodedIsNull = null;
    if (!decodeExpr.getChildren().isEmpty()) {
      encoded = decodeExpr.getChild(childIdx++);
      encodedIsNull = new IsNullPredicate(encoded, false);
    }

    // Add the key_expr/val_expr pairs
    while (childIdx + 2 <= decodeExpr.getChildren().size()) {
      Expr candidate = decodeExpr.getChild(childIdx++);
      if (candidate.isLiteral()) {
        if (candidate.isNullLiteral()) {
          // An example case is DECODE(foo, NULL, bar), since NULLs are considered
          // equal, this becomes CASE WHEN foo IS NULL THEN bar END.
          children_.add(encodedIsNull);
        } else {
          children_.add(new BinaryPredicate(
              BinaryPredicate.Operator.EQ, encoded, candidate));
        }
      } else {
        children_.add(new CompoundPredicate(CompoundPredicate.Operator.OR,
            new CompoundPredicate(CompoundPredicate.Operator.AND,
                encodedIsNull, new IsNullPredicate(candidate, false)),
            new BinaryPredicate(BinaryPredicate.Operator.EQ, encoded, candidate)));
      }

      // Add the value
      children_.add(decodeExpr.getChild(childIdx++));
    }

    // Add the default value
    if (childIdx < decodeExpr.getChildren().size()) {
      hasElseExpr_ = true;
      children_.add(decodeExpr.getChild(childIdx));
    }
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CaseExpr(CaseExpr other) {
    super(other);
    decodeExpr_ = other.decodeExpr_;
    hasCaseExpr_ = other.hasCaseExpr_;
    hasElseExpr_ = other.hasElseExpr_;
  }

  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      if (t.isScalarType(PrimitiveType.CHAR)) continue;
      // TODO: case is special and the signature cannot be represented.
      // It is alternating varargs
      // e.g. case(bool, type, bool type, bool type, etc).
      // Instead we just add a version for each of the return types
      // e.g. case(BOOLEAN), case(INT), etc
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          "case", "", Lists.newArrayList(t), t));
      // Same for DECODE
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          "decode", "", Lists.newArrayList(t), t));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }

  @Override
  public boolean equals(Object obj) {
<<<<<<< HEAD
    if (!super.equals(obj)) {
      return false;
    }
    CaseExpr expr = (CaseExpr) obj;
    return hasCaseExpr == expr.hasCaseExpr && hasElseExpr == expr.hasElseExpr;
  }

  @Override
  public String toSql() {
    StringBuilder output = new StringBuilder("CASE");
    int childIdx = 0;
    if (hasCaseExpr) {
      output.append(children.get(childIdx++).toSql());
    }
    while (childIdx + 2 <= children.size()) {
      output.append(" WHEN " + children.get(childIdx++).toSql());
      output.append(" THEN " + children.get(childIdx++).toSql());
    }
    if (hasElseExpr) {
      output.append(" ELSE " + children.get(children.size() - 1).toSql());
=======
    if (!super.equals(obj)) return false;
    CaseExpr expr = (CaseExpr) obj;
    return hasCaseExpr_ == expr.hasCaseExpr_
        && hasElseExpr_ == expr.hasElseExpr_
        && isDecode() == expr.isDecode();
  }

  @Override
  public String toSqlImpl() {
    return (decodeExpr_ == null) ? toCaseSql() : decodeExpr_.toSqlImpl();
  }

  @VisibleForTesting
  String toCaseSql() {
    StringBuilder output = new StringBuilder("CASE");
    int childIdx = 0;
    if (hasCaseExpr_) {
      output.append(" " + children_.get(childIdx++).toSql());
    }
    while (childIdx + 2 <= children_.size()) {
      output.append(" WHEN " + children_.get(childIdx++).toSql());
      output.append(" THEN " + children_.get(childIdx++).toSql());
    }
    if (hasElseExpr_) {
      output.append(" ELSE " + children_.get(children_.size() - 1).toSql());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
    output.append(" END");
    return output.toString();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CASE_EXPR;
<<<<<<< HEAD
    msg.case_expr = new TCaseExpr(hasCaseExpr, hasElseExpr);
    msg.setOpcode(opcode);
=======
    msg.case_expr = new TCaseExpr(hasCaseExpr_, hasElseExpr_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
<<<<<<< HEAD
    super.analyze(analyzer);

    // Keep track of maximum compatible type of case expr and all when exprs.
    PrimitiveType whenType = null;
    // Keep track of maximum compatible type of else expr and all then exprs.
    PrimitiveType returnType = null;
    // Remember last of these exprs for error reporting.
    Expr lastCompatibleThenExpr = null;
    Expr lastCompatibleWhenExpr = null;
    int loopEnd = children.size();
    if (hasElseExpr) {
=======
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    castChildCharsToStrings(analyzer);

    if (isDecode()) {
      Preconditions.checkState(!hasCaseExpr_);
      // decodeExpr_.analyze() would fail validating function existence. The complex
      // vararg signature is currently unsupported.
      FunctionCallExpr.validateScalarFnParams(decodeExpr_.getParams());
      if (decodeExpr_.getChildren().size() < 3) {
        throw new AnalysisException("DECODE in '" + toSql() + "' requires at least 3 "
            + "arguments.");
      }
    }

    // Keep track of maximum compatible type of case expr and all when exprs.
    Type whenType = null;
    // Keep track of maximum compatible type of else expr and all then exprs.
    Type returnType = null;
    // Remember last of these exprs for error reporting.
    Expr lastCompatibleThenExpr = null;
    Expr lastCompatibleWhenExpr = null;
    int loopEnd = children_.size();
    if (hasElseExpr_) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      --loopEnd;
    }
    int loopStart;
    Expr caseExpr = null;
    // Set loop start, and initialize returnType as type of castExpr.
<<<<<<< HEAD
    if (hasCaseExpr) {
      loopStart = 1;
      caseExpr = children.get(0);
      caseExpr.analyze(analyzer);
      whenType = caseExpr.getType();
      lastCompatibleWhenExpr = children.get(0);
    } else {
      whenType = PrimitiveType.BOOLEAN;
=======
    if (hasCaseExpr_) {
      loopStart = 1;
      caseExpr = children_.get(0);
      caseExpr.analyze(analyzer);
      whenType = caseExpr.getType();
      lastCompatibleWhenExpr = children_.get(0);
    } else {
      whenType = Type.BOOLEAN;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      loopStart = 0;
    }

    // Go through when/then exprs and determine compatible types.
    for (int i = loopStart; i < loopEnd; i += 2) {
<<<<<<< HEAD
      Expr whenExpr = children.get(i);
      if (hasCaseExpr) {
        // Determine maximum compatible type of the case expr,
        // and all when expr seen so far.
        // We will add casts to them at the very end.
=======
      Expr whenExpr = children_.get(i);
      if (hasCaseExpr_) {
        // Determine maximum compatible type of the case expr,
        // and all when exprs seen so far. We will add casts to them at the very end.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        whenType = analyzer.getCompatibleType(whenType,
            lastCompatibleWhenExpr, whenExpr);
        lastCompatibleWhenExpr = whenExpr;
      } else {
        // If no case expr was given, then the when exprs should always return
        // boolean or be castable to boolean.
<<<<<<< HEAD
        if (!PrimitiveType.isImplicitlyCastable(whenExpr.getType(),
            PrimitiveType.BOOLEAN)) {
=======
        if (!Type.isImplicitlyCastable(whenExpr.getType(),
            Type.BOOLEAN)) {
          Preconditions.checkState(isCase());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
          throw new AnalysisException("When expr '" + whenExpr.toSql() + "'" +
              " is not of type boolean and not castable to type boolean.");
        }
        // Add a cast if necessary.
<<<<<<< HEAD
        if (whenExpr.getType() != PrimitiveType.BOOLEAN) {
          castChild(PrimitiveType.BOOLEAN, i);
        }
      }
      // Determine maximum compatible type of the then exprs seen so far.
      // We will add casts to them at the very end.
      Expr thenExpr = children.get(i + 1);
=======
        if (!whenExpr.getType().isBoolean()) castChild(Type.BOOLEAN, i);
      }
      // Determine maximum compatible type of the then exprs seen so far.
      // We will add casts to them at the very end.
      Expr thenExpr = children_.get(i + 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, thenExpr);
      lastCompatibleThenExpr = thenExpr;
    }
<<<<<<< HEAD
    if (hasElseExpr) {
      Expr elseExpr = children.get(children.size() - 1);
=======
    if (hasElseExpr_) {
      Expr elseExpr = children_.get(children_.size() - 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, elseExpr);
    }

<<<<<<< HEAD
    // Add casts to case expr to compatible type.
    if (hasCaseExpr) {
      // Cast case expr.
      if (children.get(0).type != whenType) {
=======
    // Make sure BE doesn't see TYPE_NULL by picking an arbitrary type
    if (whenType.isNull()) whenType = ScalarType.BOOLEAN;
    if (returnType.isNull()) returnType = ScalarType.BOOLEAN;

    // Add casts to case expr to compatible type.
    if (hasCaseExpr_) {
      // Cast case expr.
      if (!children_.get(0).type_.equals(whenType)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        castChild(whenType, 0);
      }
      // Add casts to when exprs to compatible type.
      for (int i = loopStart; i < loopEnd; i += 2) {
<<<<<<< HEAD
        if (children.get(i).type != whenType) {
=======
        if (!children_.get(i).type_.equals(whenType)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
          castChild(whenType, i);
        }
      }
    }
    // Cast then exprs to compatible type.
<<<<<<< HEAD
    for (int i = loopStart + 1; i < children.size(); i += 2) {
      if (children.get(i).type != returnType) {
=======
    for (int i = loopStart + 1; i < children_.size(); i += 2) {
      if (!children_.get(i).type_.equals(returnType)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        castChild(returnType, i);
      }
    }
    // Cast else expr to compatible type.
<<<<<<< HEAD
    if (hasElseExpr) {
      if (children.get(children.size() - 1).type != returnType) {
        castChild(returnType, children.size() - 1);
      }
    }

    // Set opcode based on whenType.
    OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
        FunctionOperator.CASE, whenType);
    if (match == null) {
      throw new AnalysisException("Could not find match in function registry " +
          "for CASE and arg type: " + whenType);
    }
    opcode = match.opcode;
    type = returnType;
    isAnalyzed = true;
  }
=======
    if (hasElseExpr_) {
      if (!children_.get(children_.size() - 1).type_.equals(returnType)) {
        castChild(returnType, children_.size() - 1);
      }
    }

    // Do the function lookup just based on the whenType.
    Type[] args = new Type[1];
    args[0] = whenType;
    fn_ = getBuiltinFunction(analyzer, "case", args, CompareMode.IS_SUPERTYPE_OF);
    Preconditions.checkNotNull(fn_);
    type_ = returnType;
  }

  private boolean isCase() { return !isDecode(); }
  private boolean isDecode() { return decodeExpr_ != null; }

  @Override
  public Expr clone() { return new CaseExpr(this); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
