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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

<<<<<<< HEAD
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOpcode;
import com.cloudera.impala.thrift.TLikePredicate;
import com.google.common.base.Preconditions;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE", TExprOpcode.LIKE),
    RLIKE("RLIKE", TExprOpcode.REGEX),
    REGEXP("REGEXP", TExprOpcode.REGEX);

    private final String description;
    private final TExprOpcode thriftOp;

    private Operator(String description, TExprOpcode thriftOp) {
      this.description = description;
      this.thriftOp = thriftOp;
=======
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE"),
    RLIKE("RLIKE"),
    REGEXP("REGEXP");

    private final String description_;

    private Operator(String description) {
      this.description_ = description;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }

    @Override
    public String toString() {
<<<<<<< HEAD
      return description;
    }

    public TExprOpcode toThrift() {
      return thriftOp;
    }
  }
  private final Operator op;

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkNotNull(e2);
    children.add(e2);
=======
      return description_;
    }
  }

  public static void initBuiltins(Db db) {
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.LIKE.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate4LikeEPN10impala_udf15FunctionContextERKNS1_9StringValES6_", "_ZN6impala13LikePredicate11LikePrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate9LikeCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.RLIKE.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate5RegexEPN10impala_udf15FunctionContextERKNS1_9StringValES6_", "_ZN6impala13LikePredicate12RegexPrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate10RegexCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.REGEXP.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate5RegexEPN10impala_udf15FunctionContextERKNS1_9StringValES6_", "_ZN6impala13LikePredicate12RegexPrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate10RegexCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
  }

  private final Operator op_;

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkNotNull(e2);
    children_.add(e2);
    // TODO: improve with histograms?
    selectivity_ = 0.1;
  }

  /**
   * Copy c'tor used in clone().
   */
  public LikePredicate(LikePredicate other) {
    super(other);
    op_ = other.op_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public boolean equals(Object obj) {
<<<<<<< HEAD
    if (!super.equals(obj)) {
      return false;
    }
    return ((LikePredicate) obj).op == op;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
=======
    if (!super.equals(obj)) return false;
    return ((LikePredicate) obj).op_ == op_;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  protected void toThrift(TExprNode msg) {
<<<<<<< HEAD
    msg.node_type = TExprNodeType.LIKE_PRED;
    msg.setOpcode(op.toThrift());
    msg.like_pred = new TLikePredicate("\\");
=======
    msg.node_type = TExprNodeType.FUNCTION_CALL;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
<<<<<<< HEAD
    super.analyze(analyzer);
    if (getChild(0).getType() != PrimitiveType.STRING) {
      throw new AnalysisException(
          "left operand of " + op.toString() + " must be of type STRING: " + this.toSql());
    }
    if (getChild(1).getType() != PrimitiveType.STRING) {
      throw new AnalysisException(
          "right operand of " + op.toString() + " must be of type STRING: " + this.toSql());
    }

    if (getChild(1).isLiteral() && (op == Operator.RLIKE || op == Operator.REGEXP)) {
=======
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!getChild(0).getType().isStringType() && !getChild(0).getType().isNull()) {
      throw new AnalysisException(
          "left operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }
    if (!getChild(1).getType().isStringType() && !getChild(1).getType().isNull()) {
      throw new AnalysisException(
          "right operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }

    fn_ = getBuiltinFunction(analyzer, op_.toString(), collectChildReturnTypes(),
        CompareMode.IS_SUPERTYPE_OF);
    Preconditions.checkState(fn_ != null);
    Preconditions.checkState(fn_.getReturnType().isBoolean());

    if (getChild(1).isLiteral() && !getChild(1).isNullLiteral()
        && (op_ == Operator.RLIKE || op_ == Operator.REGEXP)) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      // let's make sure the pattern works
      // TODO: this checks that it's a Java-supported regex, but the syntax supported
      // by the backend is Posix; add a call to the backend to check the re syntax
      try {
        Pattern.compile(((StringLiteral) getChild(1)).getValue());
      } catch (PatternSyntaxException e) {
        throw new AnalysisException(
<<<<<<< HEAD
          "invalid regular expression in '" + this.toSql() + "'");
      }
    }
  }
=======
            "invalid regular expression in '" + this.toSql() + "'");
      }
    }
    castForFunctionCall(false);
  }

  @Override
  public Expr clone() { return new LikePredicate(this); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
