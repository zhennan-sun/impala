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
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

public abstract class Predicate extends Expr {
  protected boolean isEqJoinConjunct;

  public Predicate() {
    super();
    this.isEqJoinConjunct = false;
  }

  public boolean isEqJoinConjunct() {
    return isEqJoinConjunct;
  }

  public void setIsEqJoinConjunct(boolean v) {
    isEqJoinConjunct = v;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    type = PrimitiveType.BOOLEAN;
  }

  public List<Predicate> getConjuncts() {
    List<Predicate> list = Lists.newArrayList();
    if (this instanceof CompoundPredicate
        && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
      // TODO: we have to convert CompoundPredicate.AND to two expr trees for
      // conjuncts because NULLs are handled differently for CompoundPredicate.AND
      // and conjunct evaluation.  This is not optimal for jitted exprs because it
      // will result in two functions instead of one. Create a new CompoundPredicate
      // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
      // instead
      list.addAll(((Predicate) getChild(0)).getConjuncts());
      list.addAll(((Predicate) getChild(1)).getConjuncts());
    } else {
      list.add(this);
    }
    return list;
  }
=======
import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.Reference;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

public abstract class Predicate extends Expr {
  protected boolean isEqJoinConjunct_;

  public Predicate() {
    super();
    this.isEqJoinConjunct_ = false;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected Predicate(Predicate other) {
    super(other);
    isEqJoinConjunct_ = other.isEqJoinConjunct_;
  }

  public boolean isEqJoinConjunct() { return isEqJoinConjunct_; }
  public void setIsEqJoinConjunct(boolean v) { isEqJoinConjunct_ = v; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    type_ = Type.BOOLEAN;
    // values: true/false/null
    numDistinctValues_ = 3;
  }

  /**
   * Returns true if one of the children is a slotref (possibly wrapped in a cast)
   * and the other children are all constant. Returns the slotref in 'slotRef' and
   * its child index in 'idx'.
   * This will pick up something like "col = 5", but not "2 * col = 10", which is
   * what we want.
   */
  public boolean isSingleColumnPredicate(
      Reference<SlotRef> slotRefRef, Reference<Integer> idxRef) {
    // find slotref
    SlotRef slotRef = null;
    int i = 0;
    for (; i < children_.size(); ++i) {
      slotRef = getChild(i).unwrapSlotRef(false);
      if (slotRef != null) break;
    }
    if (slotRef == null) return false;

    // make sure everything else is constant
    for (int j = 0; j < children_.size(); ++j) {
      if (i == j) continue;
      if (!getChild(j).isConstant()) return false;
    }

    if (slotRefRef != null) slotRefRef.setRef(slotRef);
    if (idxRef != null) idxRef.setRef(Integer.valueOf(i));
    return true;
  }

  /**
   * If predicate is of the form "<slotref> = <slotref>", returns both SlotRefs,
   * otherwise returns null.
   */
  public Pair<SlotId, SlotId> getEqSlots() { return null; }

  /**
   * Returns the SlotRef bound by this Predicate.
   */
  public SlotRef getBoundSlot() { return null; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
