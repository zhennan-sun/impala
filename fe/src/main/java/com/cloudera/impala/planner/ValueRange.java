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

package com.cloudera.impala.planner;

<<<<<<< HEAD
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
<<<<<<< HEAD
import com.cloudera.impala.thrift.TExpr;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.base.Preconditions;

/**
 * Representation of a two-sided interval of values. Either one of the sides
 * is optional, and can be exclusive or inclusive. For a range representing a single
 * value, both bounds are set.
 */
public class ValueRange {
  private final static Logger LOG = LoggerFactory.getLogger(ValueRange.class);

<<<<<<< HEAD
  Expr lowerBound;
  boolean lowerBoundInclusive;
  Expr upperBound;
  boolean upperBoundInclusive;

  static public ValueRange createEqRange(Expr valueExpr) {
    ValueRange result = new ValueRange();
    result.lowerBound = valueExpr;
    result.lowerBoundInclusive = true;
    result.upperBound = valueExpr;
    result.upperBoundInclusive = true;
    return result;
  }

=======
  private Expr lowerBound_;
  private boolean lowerBoundInclusive_;
  private Expr upperBound_;
  private boolean upperBoundInclusive_;

  Expr getLowerBound() { return lowerBound_; }
  void setLowerBound(Expr e) { lowerBound_ = e; }
  boolean getLowerBoundInclusive() { return lowerBoundInclusive_; }
  void setLowerBoundInclusive(boolean b) { lowerBoundInclusive_ = b; }
  Expr getUpperBound() { return upperBound_; }
  void setUpperBound(Expr e) { upperBound_ = e; }
  boolean getUpperBoundInclusive() { return upperBoundInclusive_; }
  void setUpperBoundInclusive(boolean b) { upperBoundInclusive_ = b; }

  static public ValueRange createEqRange(Expr valueExpr) {
    ValueRange result = new ValueRange();
    result.lowerBound_ = valueExpr;
    result.lowerBoundInclusive_ = true;
    result.upperBound_ = valueExpr;
    result.upperBoundInclusive_ = true;
    return result;
  }

  public boolean isEqRange() {
    return lowerBound_ == upperBound_ && lowerBoundInclusive_ && upperBoundInclusive_;
  }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  /**
   * Determines whether a given constant expr is within the range.
   * Does this by constructing predicate that represents the range,
   * with the valueExpr inserted appropriately, and then calls the
   * backend for evaluation.
   */
<<<<<<< HEAD
  public boolean isInRange(Analyzer analyzer, Expr valueExpr) throws InternalException {
    Preconditions.checkState(valueExpr.isConstant());
    Preconditions.checkState(lowerBound != null || upperBound != null);

    // construct predicate
    Predicate p = null;
    if (lowerBound != null && upperBound != null
        && lowerBoundInclusive && upperBoundInclusive
        && lowerBound == upperBound) {
      // construct "=" predicate
      p = new BinaryPredicate(BinaryPredicate.Operator.EQ, valueExpr, lowerBound);
    } else {
      // construct range predicate
      if (lowerBound != null) {
        p = new BinaryPredicate(
            lowerBoundInclusive ? BinaryPredicate.Operator.GE : BinaryPredicate.Operator.GT,
            valueExpr, lowerBound);
      }
      if (upperBound != null) {
        Predicate p2 = new BinaryPredicate(
            upperBoundInclusive ? BinaryPredicate.Operator.GE : BinaryPredicate.Operator.GT,
            upperBound, valueExpr);
=======
  public boolean isInRange(Analyzer analyzer, Expr valueExpr) throws
      InternalException {
    Preconditions.checkState(valueExpr.isConstant());
    Preconditions.checkState(lowerBound_ != null || upperBound_ != null);

    // construct predicate
    Predicate p = null;
    if (lowerBound_ != null && upperBound_ != null
        && lowerBoundInclusive_ && upperBoundInclusive_
        && lowerBound_ == upperBound_) {
      // construct "=" predicate
      p = new BinaryPredicate(BinaryPredicate.Operator.EQ, valueExpr, lowerBound_);
    } else {
      // construct range predicate
      if (lowerBound_ != null) {
        p = new BinaryPredicate(
            lowerBoundInclusive_
              ? BinaryPredicate.Operator.GE : BinaryPredicate.Operator.GT,
            valueExpr, lowerBound_);
      }
      if (upperBound_ != null) {
        Predicate p2 = new BinaryPredicate(
            upperBoundInclusive_
              ? BinaryPredicate.Operator.GE : BinaryPredicate.Operator.GT,
            upperBound_, valueExpr);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        if (p != null) {
          p = new CompoundPredicate(CompoundPredicate.Operator.AND, p, p2);
        } else {
          p = p2;
        }
      }
    }

    Preconditions.checkState(p.isConstant());
    // analyze to insert casts, etc.
    try {
      p.analyze(analyzer);
    } catch (AnalysisException e) {
      // this should never happen
      throw new InternalException(
          "couldn't analyze predicate " + p.toSql() + "\n" + e.toString());
    }

    // call backend
<<<<<<< HEAD
    TExpr thriftExpr = p.treeToThrift();
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      boolean result = FeSupport.EvalPredicate(serializer.serialize(thriftExpr));
      return result;
    } catch (TException e) {
      // this should never happen
      throw new InternalException(
          "couldn't execute predicate " + p.toSql() + "\n" + e.toString());
    }
=======
    return FeSupport.EvalPredicate(p, analyzer.getQueryCtx());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

}
