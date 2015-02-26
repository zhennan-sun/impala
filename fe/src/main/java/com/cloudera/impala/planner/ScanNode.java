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

import java.util.List;

<<<<<<< HEAD
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.thrift.THostPort;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Objects;
=======
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

/**
 * Representation of the common elements of all scan nodes.
 */
abstract public class ScanNode extends PlanNode {
<<<<<<< HEAD
  protected final TupleDescriptor desc;

  /**
   * One range per clustering column. The range bounds are expected to be constants.
   * A null entry means there's no range restriction for that particular key.
   * Might contain fewer entries than there are keys (ie, there are no trailing
   * null entries).
   */
  protected List<ValueRange> keyRanges;

  public ScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc.getId().asList());
    this.desc = desc;
  }

  public void setKeyRanges(List<ValueRange> keyRanges) {
    if (!keyRanges.isEmpty()) {
      this.keyRanges = keyRanges;
    }
  }

  /**
   * Returns all scan ranges plus their locations. Needs to be preceded by a call to
   * finalize().
   * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
   *     only applicable to HDFS; less than or equal to zero means no maximum.
   */
  abstract public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);
=======
  protected final TupleDescriptor desc_;

  // Total number of rows this node is expected to process
  protected long inputCardinality_ = -1;

  // Counter indicating if partitions have missing statistics
  protected int numPartitionsMissingStats_ = 0;

  // List of scan-range locations. Populated in init().
  protected List<TScanRangeLocations> scanRanges_;

  public ScanNode(PlanNodeId id, TupleDescriptor desc, String displayName) {
    super(id, desc.getId().asList(), displayName);
    desc_ = desc;
  }

  public TupleDescriptor getTupleDesc() { return desc_; }

  /**
   * Returns all scan ranges plus their locations.
   */
  public List<TScanRangeLocations> getScanRangeLocations() {
    Preconditions.checkNotNull(scanRanges_, "Need to call init() first.");
    return scanRanges_;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
<<<<<<< HEAD
        .add("tid", desc.getId().asInt())
        .add("tblName", desc.getTable().getFullName())
=======
        .add("tid", desc_.getId().asInt())
        .add("tblName", desc_.getTable().getFullName())
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        .add("keyRanges", "")
        .addValue(super.debugString())
        .toString();
  }

  /**
<<<<<<< HEAD
   * Helper function to parse a "host:port" address string into THostPort
   * This is called with ipaddress:port when doing scan range assigment.
   */
  protected static THostPort addressToTHostPort(String address) {
    THostPort result = new THostPort();
    String[] hostPort = address.split(":");
    // In this context we don't have or need a hostname,
    // so we just set it to the ipaddress.
    result.hostname = hostPort[0];
    result.ipaddress = hostPort[0];
=======
   * Returns the explain string for table and columns stats to be included into the
   * a ScanNode's explain string. The given prefix is prepended to each of the lines.
   * The prefix is used for proper formatting when the string returned by this method
   * is embedded in a query's explain plan.
   */
  protected String getStatsExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    // Table stats.
    if (desc_.getTable().getNumRows() == -1) {
      output.append(prefix + "table stats: unavailable");
    } else {
      output.append(prefix + "table stats: " + desc_.getTable().getNumRows() +
          " rows total");
    }
    output.append("\n");

    // Column stats.
    List<String> columnsMissingStats = Lists.newArrayList();
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.getStats().hasStats()) {
        columnsMissingStats.add(slot.getColumn().getName());
      }
    }
    if (columnsMissingStats.isEmpty()) {
      output.append(prefix + "column stats: all");
    } else if (columnsMissingStats.size() == desc_.getSlots().size()) {
      output.append(prefix + "column stats: unavailable");
    } else {
      output.append(String.format("%scolumns missing stats: %s", prefix,
          Joiner.on(", ").join(columnsMissingStats)));
    }
    return output.toString();
  }

  /**
   * Returns true if the table underlying this scan is missing table stats
   * or column stats relevant to this scan node.
   */
  public boolean isTableMissingStats() {
    return isTableMissingColumnStats() || isTableMissingTableStats();
  }

  public boolean isTableMissingTableStats() {
    if (desc_.getTable().getNumRows() == -1) return true;
    return numPartitionsMissingStats_ > 0;
  }

  public boolean isTableMissingColumnStats() {
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.getStats().hasStats()) return true;
    }
    return false;
  }


  /**
   * Helper function to parse a "host:port" address string into TNetworkAddress
   * This is called with ipaddress:port when doing scan range assignment.
   */
  protected static TNetworkAddress addressToTNetworkAddress(String address) {
    TNetworkAddress result = new TNetworkAddress();
    String[] hostPort = address.split(":");
    result.hostname = hostPort[0];
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    result.port = Integer.parseInt(hostPort[1]);
    return result;
  }

<<<<<<< HEAD
=======
  @Override
  public long getInputCardinality() {
    if (getConjuncts().isEmpty() && hasLimit()) return getLimit();
    return inputCardinality_;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
