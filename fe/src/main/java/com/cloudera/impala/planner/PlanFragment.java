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
import java.util.ArrayList;
import java.util.HashSet;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
<<<<<<< HEAD
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
=======
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.HashJoinNode.DistributionMode;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TPlanFragment;
import com.google.common.base.Preconditions;
<<<<<<< HEAD
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
=======
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

/**
 * A PlanFragment is part of a tree of such fragments that together make
 * up a complete execution plan for a single query. Each plan fragment can have
 * one or many instances, each of which in turn is executed by a single node and the
 * output sent to a specific instance of the destination fragment (or, in the case
 * of the root fragment, is materialized in some form).
 *
 * The plan fragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * is produced by the plan root is marked as materialized.
 *
<<<<<<< HEAD
=======
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 *
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 */
public class PlanFragment {
  private final static Logger LOG = LoggerFactory.getLogger(PlanFragment.class);

<<<<<<< HEAD
  // root of plan tree executed by this fragment
  private PlanNode planRoot;

  // fragment that consumes the output of this one
  private PlanFragment destFragment;

  // id of ExchangeNode to which this fragment sends its output
  private PlanNodeId destNodeId;

  // if null, outputs the entire row produced by planRoot
  private ArrayList<Expr> outputExprs;

  // created in finalize() or set in setSink()
  private DataSink sink;
=======
  private final PlanFragmentId fragmentId_;

  // root of plan tree executed by this fragment
  private PlanNode planRoot_;

  // exchange node to which this fragment sends its output
  private ExchangeNode destNode_;

  // if null, outputs the entire row produced by planRoot_
  private List<Expr> outputExprs_;

  // created in finalize() or set in setSink()
  private DataSink sink_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // specification of the partition of the input of this fragment;
  // an UNPARTITIONED fragment is executed on only a single node
  // TODO: improve this comment, "input" is a bit misleading
<<<<<<< HEAD
  private final DataPartition dataPartition;
=======
  private DataPartition dataPartition_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // specification of how the output of this fragment is partitioned (i.e., how
  // it's sent to its destination);
  // if the output is UNPARTITIONED, it is being broadcast
<<<<<<< HEAD
  private DataPartition outputPartition;

  // TODO: SubstitutionMap outputSmap;
  // substitution map to remap exprs onto the output of this fragment, to be applied
  // at destination fragment

  /**
   * C'tor for unpartitioned fragment that produces final output
   */
  public PlanFragment(PlanNode root, ArrayList<Expr> outputExprs) {
    this.planRoot = root;
    this.outputExprs = Expr.cloneList(outputExprs, null);
    this.dataPartition = DataPartition.UNPARTITIONED;
    this.outputPartition = this.dataPartition;
  }

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanNode root, DataPartition partition) {
    this.planRoot = root;
    this.dataPartition = partition;
    this.outputPartition = DataPartition.UNPARTITIONED;
  }

  public void setOutputExprs(ArrayList<Expr> outputExprs) {
    this.outputExprs = Expr.cloneList(outputExprs, null);
  }

  /**
   * Finalize plan tree and create stream sink, if needed.
   */
  public void finalize(Analyzer analyzer, boolean validateFileFormats)
      throws InternalException, NotImplementedException {
    markRefdSlots(analyzer);
    if (planRoot != null) {
      planRoot.finalize(analyzer);
    }
    if (destNodeId != null) {
      Preconditions.checkState(sink == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink = new DataStreamSink(destNodeId);
      streamSink.setPartition(outputPartition);
      sink = streamSink;
    }

    if (planRoot != null && validateFileFormats) {
      // verify that after partition pruning hdfs partitions only use supported formats
      ArrayList<HdfsScanNode> hdfsScans = Lists.newArrayList();
      planRoot.collectSubclasses(HdfsScanNode.class, hdfsScans);
      for (HdfsScanNode hdfsScanNode: hdfsScans) {
        hdfsScanNode.validateFileFormat();
      }
    }
=======
  private DataPartition outputPartition_;

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
    fragmentId_ = id;
    planRoot_ = root;
    dataPartition_ = partition;
    outputPartition_ = DataPartition.UNPARTITIONED;
    setFragmentInPlanTree(planRoot_);
  }

  /**
   * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
   * Does not traverse the children of ExchangeNodes because those must belong to a
   * different fragment.
   */
  private void setFragmentInPlanTree(PlanNode node) {
    if (node == null) return;
    node.setFragment(this);
    if (!(node instanceof ExchangeNode)) {
      for (PlanNode child : node.getChildren()) {
        setFragmentInPlanTree(child);
      }
    }
  }

  public void setOutputExprs(List<Expr> outputExprs) {
    outputExprs_ = Expr.cloneList(outputExprs);
  }
  public List<Expr> getOutputExprs() { return outputExprs_; }

  /**
   * Finalize plan tree and create stream sink, if needed.
   * If this fragment is hash partitioned, ensures that the corresponding partition
   * exprs of all hash-partitioning senders are cast to identical types.
   * Otherwise, the hashes generated for identical partition values may differ
   * among senders if the partition-expr types are not identical.
   */
  public void finalize(Analyzer analyzer)
      throws InternalException, NotImplementedException {
    if (planRoot_ != null) computeCanAddSlotFilters(planRoot_);

    if (destNode_ != null) {
      Preconditions.checkState(sink_ == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink = new DataStreamSink(destNode_, outputPartition_);
      streamSink.setFragment(this);
      sink_ = streamSink;
    }

    if (!dataPartition_.isHashPartitioned()) return;

    // This fragment is hash partitioned. Gather all exchange nodes and ensure
    // that all hash-partitioning senders hash on exprs-values of the same type.
    List<ExchangeNode> exchNodes = Lists.newArrayList();
    planRoot_.collect(Predicates.instanceOf(ExchangeNode.class), exchNodes);

    // Contains partition-expr lists of all hash-partitioning sender fragments.
    List<List<Expr>> senderPartitionExprs = Lists.newArrayList();
    for (ExchangeNode exchNode: exchNodes) {
      Preconditions.checkState(!exchNode.getChildren().isEmpty());
      PlanFragment senderFragment = exchNode.getChild(0).getFragment();
      Preconditions.checkNotNull(senderFragment);
      if (!senderFragment.getOutputPartition().isHashPartitioned()) continue;
      List<Expr> partExprs = senderFragment.getOutputPartition().getPartitionExprs();
      // All hash-partitioning senders must have compatible partition exprs, otherwise
      // this fragment's data partition must not be hash partitioned.
      Preconditions.checkState(
          partExprs.size() == dataPartition_.getPartitionExprs().size());
      senderPartitionExprs.add(partExprs);
    }

    // Cast all corresponding hash partition exprs of all hash-partitioning senders
    // to their compatible types. Also cast the data partition's exprs for consistency,
    // although not strictly necessary. They should already be type identical to the
    // exprs of one of the senders and they are not directly used for hashing in the BE.
    senderPartitionExprs.add(dataPartition_.getPartitionExprs());
    try {
      analyzer.castToUnionCompatibleTypes(senderPartitionExprs);
    } catch (AnalysisException e) {
      // Should never happen. Analysis should have ensured type compatibility already.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Return the number of nodes on which the plan fragment will execute.
   * invalid: -1
   */
  public int getNumNodes() {
    return dataPartition_ == DataPartition.UNPARTITIONED ? 1 : planRoot_.getNumNodes();
  }

  /**
   * Returns true and sets node.canAddPredicate, if we can add single-slot filters at
   * execution time (i.e. after Prepare() to the plan tree rooted at this node.
   * That is, 'node' can add filters that can be evaluated at nodes below.
   *
   * We compute this by walking the tree bottom up.
   *
   * TODO: move this to PlanNode.init() which is normally responsible for computing
   * internal state of PlanNodes. We cannot do this currently since we need the
   * distrubutionMode() set on HashJoin nodes. Once we call init() properly for
   * repartitioned joins, this logic can move to init().
   */
  private boolean computeCanAddSlotFilters(PlanNode node) {
    if (node instanceof HashJoinNode) {
      HashJoinNode hashJoinNode = (HashJoinNode)node;
      boolean childResult = computeCanAddSlotFilters(node.getChild(0));
      if (!childResult) return false;
      if (hashJoinNode.getJoinOp().equals(JoinOperator.FULL_OUTER_JOIN) ||
          hashJoinNode.getJoinOp().equals(JoinOperator.LEFT_OUTER_JOIN) ||
          hashJoinNode.getJoinOp().equals(JoinOperator.LEFT_ANTI_JOIN) ||
          hashJoinNode.getJoinOp().equals(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)) {
        // It is not correct to push through an outer or anti join on the probe side.
        // We cannot filter those rows out.
        return false;
      }
      // We can't push down predicates for partitioned joins yet.
      // TODO: this can be hugely helpful to avoid network traffic. Implement this.
      if (hashJoinNode.getDistributionMode() == DistributionMode.PARTITIONED) {
        return false;
      }

      List<BinaryPredicate> joinConjuncts = hashJoinNode.getEqJoinConjuncts();
      // We can only add these filters for conjuncts of the form:
      // <probe_slot> = *. If the hash join has any equal join conjuncts in this form,
      // mark the hash join node.
      for (Expr c: joinConjuncts) {
        if (c.getChild(0) instanceof SlotRef) {
          hashJoinNode.setAddProbeFilters(true);
          break;
        }
      }
      // Even if this join cannot add predicates, return true so the parent node can.
      return true;
    } else if (node instanceof HdfsScanNode) {
      // Since currently only the Parquet scanner employs the slot filter optimization,
      // we enable it only if the majority format is Parquet. Otherwise we are adding
      // the overhead of creating the SlotFilters in the build side in queries not on
      // Parquet data.
      // TODO: Modify the other scanners to exploit the slot filter optimization.
      HdfsScanNode scanNode = (HdfsScanNode) node;
      Preconditions.checkNotNull(scanNode.desc_);
      Preconditions.checkNotNull(scanNode.desc_.getTable() instanceof HdfsTable);
      HdfsTable table = (HdfsTable) scanNode.desc_.getTable();
      if (table.getMajorityFormat() == HdfsFileFormat.PARQUET) {
        return true;
      } else {
        return false;
      }
    } else {
      for (PlanNode child : node.getChildren()) {
        computeCanAddSlotFilters(child);
      }
      return false;
    }
  }

  /**
   * Estimates the per-node number of distinct values of exprs based on the data
   * partition of this fragment and its number of nodes. Returns -1 for an invalid
   * estimate, e.g., because getNumDistinctValues() failed on one of the exprs.
   */
  public long getNumDistinctValues(List<Expr> exprs) {
    Preconditions.checkNotNull(dataPartition_);
    long result = 1;
    int numNodes = getNumNodes();
    Preconditions.checkState(numNodes >= 0);
    // The number of nodes is zero for empty tables.
    if (numNodes == 0) return 0;
    for (Expr expr: exprs) {
      long numDistinct = expr.getNumDistinctValues();
      if (numDistinct == -1) {
        result = -1;
        break;
      }
      if (dataPartition_.getPartitionExprs().contains(expr)) {
        numDistinct = (long)Math.max((double) numDistinct / (double) numNodes, 1L);
      }
      result = PlanNode.multiplyCardinalities(result, numDistinct);
    }
    return result;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  public TPlanFragment toThrift() {
    TPlanFragment result = new TPlanFragment();
<<<<<<< HEAD
    if (planRoot != null) {
      result.setPlan(planRoot.treeToThrift());
    }
    if (outputExprs != null) {
      result.setOutput_exprs(Expr.treesToThrift(outputExprs));
    }
    if (sink != null) {
      result.setOutput_sink(sink.toThrift());
    }
    result.setPartition(dataPartition.toThrift());
=======
    result.setDisplay_name(fragmentId_.toString());
    if (planRoot_ != null) result.setPlan(planRoot_.treeToThrift());
    if (outputExprs_ != null) {
      result.setOutput_exprs(Expr.treesToThrift(outputExprs_));
    }
    if (sink_ != null) result.setOutput_sink(sink_.toThrift());
    result.setPartition(dataPartition_.toThrift());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return result;
  }

  public String getExplainString(TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
<<<<<<< HEAD
    Preconditions.checkState(dataPartition != null);
    str.append("  " + dataPartition.getExplainString(explainLevel));
    if (sink != null) {
      str.append(sink.getExplainString("  ", explainLevel));
    }
    if (planRoot != null) {
      str.append(planRoot.getExplainString("  ", explainLevel));
=======
    Preconditions.checkState(dataPartition_ != null);
    String rootPrefix = "";
    String prefix = "";
    String detailPrefix = "|  ";
    if (explainLevel == TExplainLevel.VERBOSE) {
      prefix = "  ";
      rootPrefix = "  ";
      detailPrefix = prefix + "|  ";
      str.append(String.format("%s:PLAN FRAGMENT [%s]\n", fragmentId_.toString(),
          dataPartition_.getExplainString()));
      if (sink_ != null && sink_ instanceof DataStreamSink) {
        str.append(sink_.getExplainString(prefix, detailPrefix, explainLevel) + "\n");
      }
    }
    // Always print table sinks.
    if (sink_ != null && sink_ instanceof TableSink) {
      str.append(sink_.getExplainString(prefix, detailPrefix, explainLevel));
      if (explainLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
        str.append(prefix + "|\n");
      }
    }
    if (planRoot_ != null) {
      str.append(planRoot_.getExplainString(rootPrefix, prefix, explainLevel));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
    return str.toString();
  }

  /** Returns true if this fragment is partitioned. */
  public boolean isPartitioned() {
<<<<<<< HEAD
    return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragment getDestFragment() {
    return destFragment;
  }

  public void setDestination(PlanFragment fragment, PlanNodeId exchangeId) {
    destFragment = fragment;
    destNodeId = exchangeId;
    // TODO: check that fragment contains node w/ exchangeId
  }

  public void setSink(DataSink sink) {
    Preconditions.checkState(this.sink == null);
    Preconditions.checkNotNull(sink);
    this.sink = sink;
  }

  public PlanNodeId getDestNodeId() {
    return destNodeId;
  }

  public DataPartition getDataPartition() {
    return dataPartition;
  }

  public DataPartition getOutputPartition() {
    return outputPartition;
  }

  public void setOutputPartition(DataPartition outputPartition) {
    this.outputPartition = outputPartition;
  }

  public PlanNode getPlanRoot() {
    return planRoot;
  }

  public void setPlanRoot(PlanNode root) {
    planRoot = root;
=======
    return (dataPartition_.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragmentId getId() { return fragmentId_; }
  public PlanFragment getDestFragment() {
    if (destNode_ == null) return null;
    return destNode_.getFragment();
  }
  public ExchangeNode getDestNode() { return destNode_; }
  public DataPartition getDataPartition() { return dataPartition_; }
  public void setDataPartition(DataPartition dataPartition) {
    this.dataPartition_ = dataPartition;
  }
  public DataPartition getOutputPartition() { return outputPartition_; }
  public void setOutputPartition(DataPartition outputPartition) {
    this.outputPartition_ = outputPartition;
  }
  public PlanNode getPlanRoot() { return planRoot_; }
  public void setPlanRoot(PlanNode root) {
    planRoot_ = root;
    setFragmentInPlanTree(planRoot_);
  }

  public void setDestination(ExchangeNode destNode) { destNode_ = destNode; }
  public boolean hasSink() { return sink_ != null; }
  public DataSink getSink() { return sink_; }
  public void setSink(DataSink sink) {
    Preconditions.checkState(this.sink_ == null);
    Preconditions.checkNotNull(sink);
    sink.setFragment(this);
    this.sink_ = sink;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  /**
   * Adds a node as the new root to the plan tree. Connects the existing
   * root as the child of newRoot.
   */
  public void addPlanRoot(PlanNode newRoot) {
    Preconditions.checkState(newRoot.getChildren().size() == 1);
<<<<<<< HEAD
    newRoot.setChild(0, planRoot);
    planRoot = newRoot;
  }

  /**
   * Mark slots that are being referenced by the plan tree itself or by the
   * outputExprs exprs as materialized. If the latter is null, mark all slots in
   * planRoot's tupleIds() as being referenced. All aggregate slots are materialized.
   *
   * TODO: instead of materializing everything produced by the plan root, derived
   * referenced slots from destination fragment and add a materialization node
   * if not all output is needed by destination fragment
   * TODO 2: should the materialization decision be cost-based?
   */
  private void markRefdSlots(Analyzer analyzer) {
    if (planRoot == null) {
      return;
    }
    List<SlotId> refdIdList = Lists.newArrayList();
    planRoot.getMaterializedIds(refdIdList);

    if (outputExprs != null) {
      Expr.getIds(outputExprs, null, refdIdList);
    }

    HashSet<SlotId> refdIds = Sets.newHashSet(refdIdList);
    for (TupleDescriptor tupleDesc: analyzer.getDescTbl().getTupleDescs()) {
      for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
        if (refdIds.contains(slotDesc.getId())) {
          slotDesc.setIsMaterialized(true);
        }
      }
    }

    if (outputExprs == null) {
      // mark all slots in planRoot.getTupleIds() as materialized
      ArrayList<TupleId> tids = planRoot.getTupleIds();
      for (TupleId tid: tids) {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tid);
        for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
          slotDesc.setIsMaterialized(true);
        }
      }
    }
  }

=======
    newRoot.setChild(0, planRoot_);
    planRoot_ = newRoot;
    planRoot_.setFragment(this);
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
