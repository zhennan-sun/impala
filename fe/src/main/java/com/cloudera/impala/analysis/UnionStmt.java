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

import java.util.ArrayList;
import java.util.List;
<<<<<<< HEAD
import java.util.ListIterator;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;

/**
 * Representation of a union with its list of operands,
 * and optional order by and limit.
 * A union materializes its results, and its resultExprs
 * are slotrefs into the materialized tuple.
 */
public class UnionStmt extends QueryStmt {
=======

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a union with its list of operands, and optional order by and limit.
 * A union materializes its results, and its resultExprs are slotrefs into the
 * materialized tuple.
 * During analysis, the operands are normalized (separated into a single sequence of
 * DISTINCT followed by a single sequence of ALL operands) and unnested to the extent
 * possible. This also creates the AggregationInfo for DISTINCT operands.
 */
public class UnionStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(UnionStmt.class);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  public static enum Qualifier {
    ALL,
    DISTINCT
  }

  /**
   * Represents an operand to a union, created by the parser.
   * Contains a query statement and the all/distinct qualifier
   * of the union operator (null for the first queryStmt).
   */
  public static class UnionOperand {
<<<<<<< HEAD
    private final QueryStmt queryStmt;
    // Null for the first operand.
    private Qualifier qualifier;

    // Analyzer used for this operand. Set in analyze().
    // We must preserve the conjuncts registered in the analyzer for partition pruning.
    private Analyzer analyzer;

    public UnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
      this.queryStmt = queryStmt;
      this.qualifier = qualifier;
    }

    public void analyze(Analyzer parent) throws AnalysisException, InternalException {
      analyzer = new Analyzer(parent);
      queryStmt.analyze(analyzer);
    }

    public QueryStmt getQueryStmt() {
      return queryStmt;
    }

    public Qualifier getQualifier() {
      return qualifier;
    }

    // Used for propagating DISTINCT.
    public void setQualifier(Qualifier qualifier) {
      this.qualifier = qualifier;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }
  }

  private final List<UnionOperand> operands;

  // Single tuple materialized by the union. Set in analyze().
  private TupleId tupleId;

  public UnionStmt(List<UnionOperand> operands,
      ArrayList<OrderByElement> orderByElements, long limit) {
    super(orderByElements, limit);
    this.operands = operands;
  }

  public List<UnionOperand> getUnionOperands() {
    return operands;
=======
    private final QueryStmt queryStmt_;
    // Null for the first operand.
    private Qualifier qualifier_;

    // Analyzer used for this operand. Set in analyze().
    // We must preserve the conjuncts registered in the analyzer for partition pruning.
    private Analyzer analyzer_;

    // map from UnionStmts result slots to our resultExprs; useful during plan generation
    private final ExprSubstitutionMap smap_ = new ExprSubstitutionMap();

    // set if this operand is guaranteed to return an empty result set;
    // used in planning when assigning conjuncts
    private boolean isDropped_ = false;

    public UnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
      this.queryStmt_ = queryStmt;
      this.qualifier_ = qualifier;
    }

    public void analyze(Analyzer parent) throws AnalysisException {
      analyzer_ = new Analyzer(parent);
      queryStmt_.analyze(analyzer_);
    }

    public QueryStmt getQueryStmt() { return queryStmt_; }
    public Qualifier getQualifier() { return qualifier_; }
    // Used for propagating DISTINCT.
    public void setQualifier(Qualifier qualifier) { this.qualifier_ = qualifier; }
    public Analyzer getAnalyzer() { return analyzer_; }
    public ExprSubstitutionMap getSmap() { return smap_; }
    public void drop() { isDropped_ = true; }
    public boolean isDropped() { return isDropped_; }

    @Override
    public UnionOperand clone() {
      return new UnionOperand(queryStmt_.clone(), qualifier_);
    }

    public boolean hasAnalyticExprs() {
      if (queryStmt_ instanceof SelectStmt) {
        return ((SelectStmt) queryStmt_).hasAnalyticInfo();
      } else {
        Preconditions.checkState(queryStmt_ instanceof UnionStmt);
        return ((UnionStmt) queryStmt_).hasAnalyticExprs();
      }
    }
  }

  // before analysis, this contains the list of union operands derived verbatim
  // from the query;
  // after analysis, this contains all of distinctOperands followed by allOperands
  protected final List<UnionOperand> operands_;

  // filled during analyze(); contains all operands that need to go through
  // distinct aggregation
  protected final List<UnionOperand> distinctOperands_ = Lists.newArrayList();

  // filled during analyze(); contains all operands that can be aggregated with
  // a simple merge without duplicate elimination (also needs to merge the output
  // of the DISTINCT operands)
  protected final List<UnionOperand> allOperands_ = Lists.newArrayList();

  protected AggregateInfo distinctAggInfo_;  // only set if we have DISTINCT ops

 // Single tuple materialized by the union. Set in analyze().
  protected TupleId tupleId_;

  // set prior to unnesting
  protected String toSqlString_ = null;

  // true if any of the operands_ references an AnalyticExpr
  private boolean hasAnalyticExprs_ = false;

  public UnionStmt(List<UnionOperand> operands,
      ArrayList<OrderByElement> orderByElements, LimitElement limitElement) {
    super(orderByElements, limitElement);
    this.operands_ = operands;
  }

  public List<UnionOperand> getOperands() { return operands_; }
  public List<UnionOperand> getDistinctOperands() { return distinctOperands_; }
  public boolean hasDistinctOps() { return !distinctOperands_.isEmpty(); }
  public List<UnionOperand> getAllOperands() { return allOperands_; }
  public boolean hasAllOps() { return !allOperands_.isEmpty(); }
  public AggregateInfo getDistinctAggInfo() { return distinctAggInfo_; }
  public boolean hasAnalyticExprs() { return hasAnalyticExprs_; }

  public void removeAllOperands() {
    operands_.removeAll(allOperands_);
    allOperands_.clear();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  /**
   * Propagates DISTINCT from left to right, and checks that all
   * union operands are union compatible, adding implicit casts if necessary.
   */
  @Override
<<<<<<< HEAD
  public void analyze(Analyzer analyzer)
      throws AnalysisException, InternalException {
    Preconditions.checkState(operands.size() > 0);

    // Propagates DISTINCT from left to right,
    propagateDistinct();

    // Make sure all operands return an equal number of exprs.
    QueryStmt firstQuery = operands.get(0).getQueryStmt();
    operands.get(0).analyze(analyzer);
    List<Expr> firstQueryExprs = firstQuery.getResultExprs();
    for (int i = 1; i < operands.size(); ++i) {
      QueryStmt query = operands.get(i).getQueryStmt();
      operands.get(i).analyze(analyzer);
      List<Expr> exprs = query.getResultExprs();
      if (firstQueryExprs.size() != exprs.size()) {
        throw new AnalysisException("Select blocks have unequal number of columns:\n" +
            "'" + firstQuery.toSql() + "' has " +
            firstQueryExprs.size() + " column(s)\n" +
            "'" + query.toSql() + "' has " + exprs.size() + " column(s)");
      }
    }

    // Determine compatible types for exprs, position by position.
    for (int i = 0; i < firstQueryExprs.size(); ++i) {
      // Type compatible with the i-th exprs of all selects.
      // Initialize with type of i-th expr in first select.
      PrimitiveType compatibleType = firstQueryExprs.get(i).getType();
      // Remember last compatible expr for error reporting.
      Expr lastCompatibleExpr = firstQueryExprs.get(i);
      for (int j = 1; j < operands.size(); ++j) {
        List<Expr> resultExprs = operands.get(j).getQueryStmt().getResultExprs();
        compatibleType = analyzer.getCompatibleType(compatibleType,
            lastCompatibleExpr, resultExprs.get(i));
        lastCompatibleExpr = resultExprs.get(i);
      }
      // Now that we've found a compatible type, add implicit casts if necessary.
      for (int j = 0; j < operands.size(); ++j) {
        List<Expr> resultExprs = operands.get(j).getQueryStmt().getResultExprs();
        if (resultExprs.get(i).getType() != compatibleType) {
          Expr castExpr = resultExprs.get(i).castTo(compatibleType);
          resultExprs.set(i, castExpr);
        }
      }
    }

    // Create tuple descriptor materialized by this UnionStmt,
    // its resultExprs, and its sortInfo if necessary.
    createTupleAndResultExprs(analyzer);
    createSortInfo(analyzer);
  }

  /**
   * Propagates DISTINCT (if present) from left to right.
   */
  private void propagateDistinct() {
    int firstDistinctPos = -1;
    for (int i = operands.size() - 1; i > 0; --i) {
      UnionOperand operand = operands.get(i);
      if (firstDistinctPos != -1) {
        // There is a DISTINCT somewhere to the right.
        operand.setQualifier(Qualifier.DISTINCT);
      } else if (operand.getQualifier() == Qualifier.DISTINCT) {
        firstDistinctPos = i;
=======
  public void analyze(Analyzer analyzer) throws AnalysisException {
    try {
      super.analyze(analyzer);
    } catch (AnalysisException e) {
      if (analyzer.getMissingTbls().isEmpty()) throw e;
    }
    Preconditions.checkState(operands_.size() > 0);

    // Propagates DISTINCT from right to left
    propagateDistinct();

    // Make sure all operands return an equal number of exprs.
    QueryStmt firstQuery = operands_.get(0).getQueryStmt();

    try {
      operands_.get(0).analyze(analyzer);
    } catch (AnalysisException e) {
      if (analyzer.getMissingTbls().isEmpty()) throw e;
    }

    List<List<Expr>> resultExprLists = Lists.newArrayList();
    List<Expr> firstQueryExprs = firstQuery.getBaseTblResultExprs();
    resultExprLists.add(firstQueryExprs);
    for (int i = 1; i < operands_.size(); ++i) {
      QueryStmt query = operands_.get(i).getQueryStmt();
      try {
        operands_.get(i).analyze(analyzer);
        List<Expr> exprs = query.getBaseTblResultExprs();
        if (firstQueryExprs.size() != exprs.size()) {
          throw new AnalysisException("Operands have unequal number of columns:\n" +
              "'" + queryStmtToSql(firstQuery) + "' has " +
              firstQueryExprs.size() + " column(s)\n" +
              "'" + queryStmtToSql(query) + "' has " + exprs.size() + " column(s)");
        }
        resultExprLists.add(exprs);
      } catch (AnalysisException e) {
        if (analyzer.getMissingTbls().isEmpty()) throw e;
      }
    }

    if (!analyzer.getMissingTbls().isEmpty()) {
      throw new AnalysisException("Found missing tables. Aborting analysis.");
    }

    // compute hasAnalyticExprs_
    hasAnalyticExprs_ = false;
    for (UnionOperand op: operands_) {
      if (op.hasAnalyticExprs()) {
        hasAnalyticExprs_ = true;
        break;
      }
    }

    analyzer.castToUnionCompatibleTypes(resultExprLists);

    // Create tuple descriptor materialized by this UnionStmt,
    // its resultExprs, and its sortInfo if necessary.
    createMetadata(analyzer);
    createSortInfo(analyzer);
    toSqlString_ = toSql();

    unnestOperands(analyzer);
    if (evaluateOrderBy_) createSortTupleInfo(analyzer);
    baseTblResultExprs_ = resultExprs_;
  }

  /**
   * Marks the baseTblResultExprs of its operands as materialized, based on
   * which of the output slots have been marked.
   * Calls materializeRequiredSlots() on the operands themselves.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) throws InternalException {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId_);
    if (!distinctOperands_.isEmpty()) {
      // to keep things simple we materialize all grouping exprs = output slots,
      // regardless of what's being referenced externally
      for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
        slotDesc.setIsMaterialized(true);
      }
    }

    if (evaluateOrderBy_) {
      sortInfo_.materializeRequiredSlots(analyzer, null);
    }

    // collect operands' result exprs
    List<SlotDescriptor> outputSlots = tupleDesc.getSlots();
    List<Expr> exprs = Lists.newArrayList();
    for (int i = 0; i < outputSlots.size(); ++i) {
      SlotDescriptor slotDesc = outputSlots.get(i);
      if (!slotDesc.isMaterialized()) continue;
      for (UnionOperand op: operands_) {
        exprs.add(op.getQueryStmt().getBaseTblResultExprs().get(i));
      }
      if (distinctAggInfo_ != null) {
        // also mark the corresponding slot in the distinct agg tuple as being
        // materialized
        distinctAggInfo_.getOutputTupleDesc().getSlots().get(i).setIsMaterialized(true);
      }
    }
    materializeSlots(analyzer, exprs);

    for (UnionOperand op: operands_) {
      op.getQueryStmt().materializeRequiredSlots(analyzer);
    }
  }

  /**
   * Fill distinct-/allOperands and performs possible unnesting of UnionStmt
   * operands in the process.
   */
  private void unnestOperands(Analyzer analyzer) throws AnalysisException {
    if (operands_.size() == 1) {
      // ValuesStmt for a single row.
      allOperands_.add(operands_.get(0));
      setOperandSmap(operands_.get(0), analyzer);
      return;
    }

    // find index of first ALL operand
    int firstUnionAllIdx = operands_.size();
    for (int i = 1; i < operands_.size(); ++i) {
      UnionOperand operand = operands_.get(i);
      if (operand.getQualifier() == Qualifier.ALL) {
        firstUnionAllIdx = (i == 1 ? 0 : i);
        break;
      }
    }
    // operands[0] is always implicitly ALL, so operands[1] can't be the
    // first one
    Preconditions.checkState(firstUnionAllIdx != 1);

    // unnest DISTINCT operands
    Preconditions.checkState(distinctOperands_.isEmpty());
    for (int i = 0; i < firstUnionAllIdx; ++i) {
      unnestOperand(distinctOperands_, Qualifier.DISTINCT, operands_.get(i));
    }

    // unnest ALL operands
    Preconditions.checkState(allOperands_.isEmpty());
    for (int i = firstUnionAllIdx; i < operands_.size(); ++i) {
      unnestOperand(allOperands_, Qualifier.ALL, operands_.get(i));
    }

    operands_.clear();
    operands_.addAll(distinctOperands_);
    operands_.addAll(allOperands_);

    // create unnested operands' smaps
    for (UnionOperand operand: operands_) {
      setOperandSmap(operand, analyzer);
    }

    // create distinctAggInfo, if necessary
    if (!distinctOperands_.isEmpty()) {
      // Aggregate produces exactly the same tuple as the original union stmt.
      ArrayList<Expr> groupingExprs = Expr.cloneList(resultExprs_);
      try {
        distinctAggInfo_ =
            AggregateInfo.create(groupingExprs, null,
              analyzer.getDescTbl().getTupleDesc(tupleId_), analyzer);
      } catch (AnalysisException e) {
        // this should never happen
        throw new AnalysisException("error creating agg info in UnionStmt.analyze()");
      }
    }
  }

  /**
   * Sets the smap for the given operand. It maps from the output slots this union's
   * tuple to the corresponding base table exprs of the operand.
   */
  private void setOperandSmap(UnionOperand operand, Analyzer analyzer) {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId_);
    // operands' smaps were already set in the operands' analyze()
    operand.getSmap().clear();
    for (int i = 0; i < tupleDesc.getSlots().size(); ++i) {
      SlotDescriptor outputSlot = tupleDesc.getSlots().get(i);
      operand.getSmap().put(
          new SlotRef(outputSlot),
          // TODO: baseTblResultExprs?
          operand.getQueryStmt().getResultExprs().get(i).clone());
    }
  }

  /**
   * Add a single operand to the target list; if the operand itself is a UnionStmt,
   * apply unnesting to the extent possible (possibly modifying 'operand' in the process).
   */
  private void unnestOperand(
      List<UnionOperand> target, Qualifier targetQualifier, UnionOperand operand) {
    QueryStmt queryStmt = operand.getQueryStmt();
    if (queryStmt instanceof SelectStmt) {
      target.add(operand);
      return;
    }

    Preconditions.checkState(queryStmt instanceof UnionStmt);
    UnionStmt unionStmt = (UnionStmt) queryStmt;
    if (unionStmt.hasLimit() || unionStmt.hasOffset()) {
      // we must preserve the nested Union
      target.add(operand);
    } else if (targetQualifier == Qualifier.DISTINCT || !unionStmt.hasDistinctOps()) {
      // there is no limit in the nested Union and we can absorb all of its
      // operands as-is
      target.addAll(unionStmt.getDistinctOperands());
      target.addAll(unionStmt.getAllOperands());
    } else {
      // the nested Union contains some Distinct ops and we're accumulating
      // into our All ops; unnest only the All ops and leave the rest in place
      target.addAll(unionStmt.getAllOperands());
      unionStmt.removeAllOperands();
      target.add(operand);
    }
  }

  /**
   * String representation of queryStmt used in reporting errors.
   * Allow subclasses to override this.
   */
  protected String queryStmtToSql(QueryStmt queryStmt) {
    return queryStmt.toSql();
  }

  /**
   * Propagates DISTINCT (if present) from right to left.
   * Implied associativity:
   * A UNION ALL B UNION DISTINCT C = (A UNION ALL B) UNION DISTINCT C
   * = A UNION DISTINCT B UNION DISTINCT C
   */
  private void propagateDistinct() {
    int lastDistinctPos = -1;
    for (int i = operands_.size() - 1; i > 0; --i) {
      UnionOperand operand = operands_.get(i);
      if (lastDistinctPos != -1) {
        // There is a DISTINCT somewhere to the right.
        operand.setQualifier(Qualifier.DISTINCT);
      } else if (operand.getQualifier() == Qualifier.DISTINCT) {
        lastDistinctPos = i;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      }
    }
  }

  /**
   * Create a descriptor for the tuple materialized by the union.
   * Set resultExprs to be slot refs into that tuple.
   * Also fills the substitution map, such that "order by" can properly resolve
   * column references from the result of the union.
<<<<<<< HEAD
   *
   * @param analyzer
   * @throws AnalysisException
   */
  private void createTupleAndResultExprs(Analyzer analyzer) throws AnalysisException {
    // Create tuple descriptor for materialized tuple created by the union.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor();
    tupleDesc.setIsMaterialized(true);
    tupleId = tupleDesc.getId();
    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands.get(0).getQueryStmt().getResultExprs();
    for (int i = 0; i < firstSelectExprs.size(); ++i) {
      SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(tupleDesc);
      slotDesc.setType(firstSelectExprs.get(i).getType());
      SlotRef slotRef = new SlotRef(slotDesc);
      resultExprs.add(slotRef);
      // Add to the substitution map so that column refs in "order by" can be resolved.
      if (orderByElements != null) {
        SlotRef aliasRef = new SlotRef(null, getColLabels().get(i));
        if (aliasSMap.lhs.contains(aliasRef)) {
          ambiguousAliasList.add(aliasRef);
        } else {
          aliasSMap.lhs.add(aliasRef);
          aliasSMap.rhs.add(slotRef);
        }
      }
    }
  }

  /**
   * Substitute exprs of the form "<number>" with the corresponding
   * expressions from the resultExprs.
   * @param exprs
   * @param errorPrefix
   * @throws AnalysisException
   */
  @Override
  protected void substituteOrdinals(List<Expr> exprs, String errorPrefix)
      throws AnalysisException {
    // Substitute ordinals.
    ListIterator<Expr> i = exprs.listIterator();
    while (i.hasNext()) {
      Expr expr = i.next();
      if (!(expr instanceof IntLiteral)) {
        continue;
      }
      long pos = ((IntLiteral) expr).getValue();
      if (pos < 1) {
        throw new AnalysisException(
            errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
      }
      if (pos > resultExprs.size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      // Create copy to protect against accidentally shared state.
      i.set(resultExprs.get((int)pos - 1).clone());
    }
  }

  public TupleId getTupleId() {
    return tupleId;
  }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    tupleIdList.add(tupleId);
=======
   */
  private void createMetadata(Analyzer analyzer) throws AnalysisException {
    // Create tuple descriptor for materialized tuple created by the union.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("union");
    tupleDesc.setIsMaterialized(true);
    tupleId_ = tupleDesc.getId();
    LOG.trace("UnionStmt.createMetadata: tupleId=" + tupleId_.toString());

    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands_.get(0).getQueryStmt().getBaseTblResultExprs();

    // Compute column stats for the materialized slots from the source exprs.
    List<ColumnStats> columnStats = Lists.newArrayList();
    for (int i = 0; i < operands_.size(); ++i) {
      List<Expr> selectExprs = operands_.get(i).getQueryStmt().getBaseTblResultExprs();
      for (int j = 0; j < selectExprs.size(); ++j) {
        ColumnStats statsToAdd = ColumnStats.fromExpr(selectExprs.get(j));
        if (i == 0) {
          columnStats.add(statsToAdd);
        } else {
          columnStats.get(j).add(statsToAdd);
        }
      }
    }

    // Create tuple descriptor and slots.
    for (int i = 0; i < firstSelectExprs.size(); ++i) {
      Expr expr = firstSelectExprs.get(i);
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(getColLabels().get(i));
      slotDesc.setType(expr.getType());
      slotDesc.setStats(columnStats.get(i));
      SlotRef outputSlotRef = new SlotRef(slotDesc);
      resultExprs_.add(outputSlotRef);

      // Add to aliasSMap so that column refs in "order by" can be resolved.
      if (orderByElements_ != null) {
        SlotRef aliasRef = new SlotRef(null, getColLabels().get(i));
        if (aliasSmap_.containsMappingFor(aliasRef)) {
          ambiguousAliasList_.add(aliasRef);
        } else {
          aliasSmap_.put(aliasRef, outputSlotRef);
        }
      }

      // register single-directional value transfers from output slot
      // to operands' result exprs (if those happen to be slotrefs);
      // don't do that if the operand computes analytic exprs
      // (see Planner.createInlineViewPlan() for the reasoning)
      for (UnionOperand op: operands_) {
        if (op.hasAnalyticExprs()) continue;
        Expr resultExpr = op.getQueryStmt().getBaseTblResultExprs().get(i);
        SlotRef slotRef = resultExpr.unwrapSlotRef(true);
        if (slotRef == null) continue;
        analyzer.registerValueTransfer(outputSlotRef.getSlotId(), slotRef.getSlotId());
      }
    }
    baseTblResultExprs_ = resultExprs_;
  }

  public TupleId getTupleId() { return tupleId_; }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    // Return the sort tuple if there is an evaluated order by.
    if (evaluateOrderBy_) {
      tupleIdList.add(sortInfo_.getSortTupleDescriptor().getId());
    } else {
      tupleIdList.add(tupleId_);
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public String toSql() {
<<<<<<< HEAD
    StringBuilder strBuilder = new StringBuilder();
    Preconditions.checkState(operands.size() > 0);
    strBuilder.append(operands.get(0).getQueryStmt().toSql());
    for (int i = 1; i < operands.size() - i; ++i) {
      strBuilder.append(" UNION " +
          ((operands.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
      if (operands.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append("(");
      }
      strBuilder.append(operands.get(i).getQueryStmt().toSql());
      if (operands.get(i).getQueryStmt() instanceof UnionStmt) {
=======
    if (toSqlString_ != null) return toSqlString_;
    StringBuilder strBuilder = new StringBuilder();
    Preconditions.checkState(operands_.size() > 0);

    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql());
      strBuilder.append(" ");
    }

    strBuilder.append(operands_.get(0).getQueryStmt().toSql());
    for (int i = 1; i < operands_.size() - 1; ++i) {
      strBuilder.append(" UNION " +
          ((operands_.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
      if (operands_.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append("(");
      }
      strBuilder.append(operands_.get(i).getQueryStmt().toSql());
      if (operands_.get(i).getQueryStmt() instanceof UnionStmt) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        strBuilder.append(")");
      }
    }
    // Determine whether we need parenthesis around the last union operand.
<<<<<<< HEAD
    UnionOperand lastOperand = operands.get(operands.size() - 1);
=======
    UnionOperand lastOperand = operands_.get(operands_.size() - 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
    strBuilder.append(" UNION " +
        ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
    if (lastQueryStmt instanceof UnionStmt ||
<<<<<<< HEAD
        ((hasOrderByClause() || hasLimitClause()) && !lastQueryStmt.hasLimitClause() &&
=======
        ((hasOrderByClause() || hasLimit() || hasOffset()) &&
            !lastQueryStmt.hasLimit() && !lastQueryStmt.hasOffset() &&
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
            !lastQueryStmt.hasOrderByClause())) {
      strBuilder.append("(");
      strBuilder.append(lastQueryStmt.toSql());
      strBuilder.append(")");
    } else {
      strBuilder.append(lastQueryStmt.toSql());
    }
    // Order By clause
    if (hasOrderByClause()) {
      strBuilder.append(" ORDER BY ");
<<<<<<< HEAD
      for (int i = 0; i < orderByElements.size(); ++i) {
        strBuilder.append(orderByElements.get(i).getExpr().toSql());
        strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " ASC" : " DESC");
        strBuilder.append((i+1 != orderByElements.size()) ? ", " : "");
      }
    }
    // Limit clause.
    if (hasLimitClause()) {
      strBuilder.append(" LIMIT ");
      strBuilder.append(limit);
    }
=======
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql());
        strBuilder.append((i+1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return strBuilder.toString();
  }

  @Override
  public ArrayList<String> getColLabels() {
<<<<<<< HEAD
    Preconditions.checkState(operands.size() > 0);
    return operands.get(0).getQueryStmt().getColLabels();
=======
    Preconditions.checkState(operands_.size() > 0);
    return operands_.get(0).getQueryStmt().getColLabels();
  }

  @Override
  public QueryStmt clone() {
    List<UnionOperand> operandClones = Lists.newArrayList();
    for (UnionOperand operand: operands_) {
      operandClones.add(operand.clone());
    }
    UnionStmt unionClone = new UnionStmt(operandClones, cloneOrderByElements(),
        limitElement_ == null ? null : limitElement_.clone());
    unionClone.setWithClause(cloneWithClause());
    unionClone.hasAnalyticExprs_ = hasAnalyticExprs_;
    return unionClone;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
}
