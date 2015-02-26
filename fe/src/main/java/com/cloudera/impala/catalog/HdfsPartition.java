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

package com.cloudera.impala.catalog;

<<<<<<< HEAD
import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.THdfsPartition;
=======
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.ToSqlUtils;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.PartitionKeyValue;
import com.cloudera.impala.catalog.PartitionStatsUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TAccessLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.THdfsCompression;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsFileDesc;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.TTableStats;
import com.cloudera.impala.thrift.TPartitionStats;
import com.cloudera.impala.util.HdfsCachingUtil;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
<<<<<<< HEAD

/**
 * Query-relevant information for one table partition. Not thread safe due to a static
 * counter that's incremented for every time the constructor is called.
 */
public class HdfsPartition {
  /**
   * Metadata for a single file in this partition - the full path and the length of the
   * file.
   */
  static public class FileDescriptor {
    private final String filePath;
    private final long fileLength;

    public String getFilePath() { return filePath; }
    public long getFileLength() { return fileLength; }

    public FileDescriptor(String filePath, long fileLength) {
      Preconditions.checkNotNull(filePath);
      Preconditions.checkArgument(fileLength >= 0);
      this.filePath = filePath;
      this.fileLength = fileLength;
=======
import com.google.common.collect.Maps;
import com.google.common.base.Joiner;

/**
 * Query-relevant information for one table partition. Partitions are comparable
 * based on their partition-key values. The comparison orders partitions in ascending
 * order with NULLs sorting last. The ordering is useful for displaying partitions
 * in SHOW statements.
 */
public class HdfsPartition implements Comparable<HdfsPartition> {
  /**
   * Metadata for a single file in this partition.
   * TODO: Do we even need this class? Just get rid of it and use the Thrift version?
   */
  static public class FileDescriptor {
    private final THdfsFileDesc fileDescriptor_;

    public String getFileName() { return fileDescriptor_.getFile_name(); }
    public long getFileLength() { return fileDescriptor_.getLength(); }
    public THdfsCompression getFileCompression() {
      return fileDescriptor_.getCompression();
    }
    public long getModificationTime() {
      return fileDescriptor_.getLast_modification_time();
    }
    public List<THdfsFileBlock> getFileBlocks() {
      return fileDescriptor_.getFile_blocks();
    }

    public THdfsFileDesc toThrift() { return fileDescriptor_; }

    public FileDescriptor(String fileName, long fileLength, long modificationTime) {
      Preconditions.checkNotNull(fileName);
      Preconditions.checkArgument(fileLength >= 0);
      fileDescriptor_ = new THdfsFileDesc();
      fileDescriptor_.setFile_name(fileName);
      fileDescriptor_.setLength(fileLength);
      fileDescriptor_.setLast_modification_time(modificationTime);
      fileDescriptor_.setCompression(
          HdfsCompression.fromFileName(fileName).toThrift());
      List<THdfsFileBlock> emptyFileBlockList = Lists.newArrayList();
      fileDescriptor_.setFile_blocks(emptyFileBlockList);
    }

    private FileDescriptor(THdfsFileDesc fileDesc) {
      this(fileDesc.getFile_name(), fileDesc.length, fileDesc.last_modification_time);
      for (THdfsFileBlock block: fileDesc.getFile_blocks()) {
        fileDescriptor_.addToFile_blocks(block);
      }
    }

    public void addFileBlock(FileBlock blockMd) {
      fileDescriptor_.addToFile_blocks(blockMd.toThrift());
    }

    public static FileDescriptor fromThrift(THdfsFileDesc desc) {
      return new FileDescriptor(desc);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }

    @Override
    public String toString() {
<<<<<<< HEAD
      return Objects.toStringHelper(this).add("Path", filePath)
          .add("Length", fileLength).toString();
    }
  }

  private final List<LiteralExpr> partitionKeyValues;

  private static long partitionIdCounter = 0;

  // A unique ID for each partition, used to identify a partition in the thrift
  // representation of a table.
  private final long id;
=======
      return Objects.toStringHelper(this)
          .add("FileName", getFileName())
          .add("Length", getFileLength()).toString();
    }
  }

  /**
   * File Block metadata
   */
  public static class FileBlock {
    private final THdfsFileBlock fileBlock_;

    private FileBlock(THdfsFileBlock fileBlock) {
      this.fileBlock_ = fileBlock;
    }

    /**
     * Construct a FileBlock given the start offset (in bytes) of the file associated
     * with this block, the length of the block (in bytes), and the set of host IDs
     * that contain replicas of this block. Host IDs are assigned when loading the
     * block metadata in HdfsTable. Does not fill diskIds.
     */
    public FileBlock(long offset, long blockLength, List<Integer> replicaHostIdxs) {
      Preconditions.checkNotNull(replicaHostIdxs);
      fileBlock_ = new THdfsFileBlock();
      fileBlock_.setOffset(offset);
      fileBlock_.setLength(blockLength);
      fileBlock_.setReplica_host_idxs(replicaHostIdxs);
    }

    public long getOffset() { return fileBlock_.getOffset(); }
    public long getLength() { return fileBlock_.getLength(); }
    public List<Integer> getReplicaHostIdxs() {
      return fileBlock_.getReplica_host_idxs();
    }

    /**
     * Populates the given THdfsFileBlock's list of disk ids with the given disk id
     * values. The number of disk ids must match the number of network addresses
     * set in the file block.
     */
    public static void setDiskIds(int[] diskIds, THdfsFileBlock fileBlock) {
      Preconditions.checkArgument(
          diskIds.length == fileBlock.getReplica_host_idxs().size());
      fileBlock.setDisk_ids(Arrays.asList(ArrayUtils.toObject(diskIds)));
    }

    /**
     * Return the disk id of the block in BlockLocation.getNames()[hostIndex]; -1 if
     * disk id is not supported.
     */
    public int getDiskId(int hostIndex) {
      if (fileBlock_.disk_ids == null) return -1;
      Preconditions.checkArgument(hostIndex >= 0);
      Preconditions.checkArgument(hostIndex < fileBlock_.getDisk_idsSize());
      return fileBlock_.getDisk_ids().get(hostIndex);
    }

    public THdfsFileBlock toThrift() { return fileBlock_; }

    public static FileBlock fromThrift(THdfsFileBlock thriftFileBlock) {
      return new FileBlock(thriftFileBlock);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("offset", fileBlock_.offset)
          .add("length", fileBlock_.length)
          .add("#disks", fileBlock_.getDisk_idsSize())
          .toString();
    }
  }

  private final HdfsTable table_;
  private final List<LiteralExpr> partitionKeyValues_;
  // estimated number of rows in partition; -1: unknown
  private long numRows_ = -1;
  private static AtomicLong partitionIdCounter_ = new AtomicLong();

  // A unique ID for each partition, used to identify a partition in the thrift
  // representation of a table.
  private final long id_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  /*
   * Note: Although you can write multiple formats to a single partition (by changing
   * the format before each write), Hive won't let you read that data and neither should
   * we. We should therefore treat mixing formats inside one partition as user error.
   * It's easy to add per-file metadata to FileDescriptor if this changes.
   */
<<<<<<< HEAD
  private final HdfsStorageDescriptor fileFormatDescriptor;

  private final List<HdfsPartition.FileDescriptor> fileDescriptors;

  public HdfsStorageDescriptor getInputFormatDescriptor() { return fileFormatDescriptor; }

  public long getId() { return id; }

  /**
   * Returns an immutable list of partition key expressions
   */
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues; }

  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors;
  }

  public List<LiteralExpr> getPartitionKeyValues() {
    return partitionKeyValues;
  }

  private HdfsPartition(List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id) {
    this.partitionKeyValues = ImmutableList.copyOf(partitionKeyValues);
    this.fileDescriptors = ImmutableList.copyOf(fileDescriptors);
    this.fileFormatDescriptor = fileFormatDescriptor;
    this.id = id;
  }

  public HdfsPartition(List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors) {
    this(partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter++);
  }

  public static HdfsPartition defaultPartition(
      HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    HdfsPartition partition = new HdfsPartition(emptyExprList,
        storageDescriptor, emptyFileDescriptorList, Constants.DEFAULT_PARTITION_ID);
    return partition;
=======
  private final HdfsStorageDescriptor fileFormatDescriptor_;
  private final org.apache.hadoop.hive.metastore.api.Partition msPartition_;
  private final List<FileDescriptor> fileDescriptors_;
  private final String location_;
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartition.class);
  private boolean isDirty_ = false;
  // True if this partition is marked as cached. Does not necessarily mean the data is
  // cached.
  private boolean isMarkedCached_ = false;
  private final TAccessLevel accessLevel_;

  // (k,v) pairs of parameters for this partition, stored in the HMS. Used by Impala to
  // store intermediate state for statistics computations.
  private Map<String, String> hmsParameters_;

  public HdfsStorageDescriptor getInputFormatDescriptor() {
    return fileFormatDescriptor_;
  }

  /**
   * Returns the metastore.api.Partition object this HdfsPartition represents. Returns
   * null if this is the default partition, or if this belongs to a unpartitioned
   * table.
   */
  public org.apache.hadoop.hive.metastore.api.Partition getMetaStorePartition() {
    return msPartition_;
  }

  public boolean isDefaultPartition() {
    return id_ == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID;
  }

  /**
   * Return a partition name formed by concatenating partition keys and their values,
   * compatible with the way Hive names partitions. Reuses Hive's
   * org.apache.hadoop.hive.common.FileUtils.makePartName() function to build the name
   * string because there are a number of special cases for how partition names are URL
   * escaped.
   * TODO: Consider storing the PartitionKeyValue in HdfsPartition. It would simplify
   * this code would be useful in other places, such as fromThrift().
   */
  public String getPartitionName() {
    List<String> partitionCols = Lists.newArrayList();
    List<String> partitionValues = Lists.newArrayList();
    for (int i = 0; i < getTable().getNumClusteringCols(); ++i) {
      partitionCols.add(getTable().getColumns().get(i).getName());
    }

    for (LiteralExpr partValue: getPartitionValues()) {
      partitionValues.add(PartitionKeyValue.getPartitionKeyValueString(partValue,
          getTable().getNullPartitionKeyValue()));
    }
    return org.apache.hadoop.hive.common.FileUtils.makePartName(
        partitionCols, partitionValues);
  }

  /**
   * Utility method which returns a string of conjuncts of equality exprs to exactly
   * select this partition (e.g. ((month=2009) AND (year=2012)).
   * TODO: Remove this when TODO elsewhere in this file to save and expose the list of
   * TPartitionKeyValues has been resolved.
   */
  public String getConjunctSql() {
    List<String> partitionCols = Lists.newArrayList();
    List<String> partitionValues = Lists.newArrayList();
    for (int i = 0; i < getTable().getNumClusteringCols(); ++i) {
      partitionCols.add(ToSqlUtils.getIdentSql(getTable().getColumns().get(i).getName()));
    }

    List<String> conjuncts = Lists.newArrayList();
    for (int i = 0; i < partitionCols.size(); ++i) {
      LiteralExpr expr = getPartitionValues().get(i);
      String sql = expr.toSql();
      if (expr instanceof NullLiteral || sql.isEmpty()) {
        conjuncts.add(ToSqlUtils.getIdentSql(partitionCols.get(i))
            + " IS NULL");
      } else {
        conjuncts.add(ToSqlUtils.getIdentSql(partitionCols.get(i))
            + "=" + sql);
      }
    }
    return "(" + Joiner.on(" AND " ).join(conjuncts) + ")";
  }

  /**
   * Returns a string of the form part_key1=value1/part_key2=value2...
   */
  public String getValuesAsString() {
    StringBuilder partDescription = new StringBuilder();
    for (int i = 0; i < getTable().getNumClusteringCols(); ++i) {
      String columnName = getTable().getColumns().get(i).getName();
      String value = PartitionKeyValue.getPartitionKeyValueString(
          getPartitionValues().get(i),
          getTable().getNullPartitionKeyValue());
      partDescription.append(columnName + "=" + value);
      if (i != getTable().getNumClusteringCols() - 1) partDescription.append("/");
    }
    return partDescription.toString();
  }

  /**
   * Returns the storage location (HDFS path) of this partition. Should only be called
   * for partitioned tables.
   */
  public String getLocation() { return location_; }
  public long getId() { return id_; }
  public HdfsTable getTable() { return table_; }
  public void setNumRows(long numRows) { numRows_ = numRows; }
  public long getNumRows() { return numRows_; }
  public boolean isMarkedCached() { return isMarkedCached_; }
  void markCached() { isMarkedCached_ = true; }

  // May return null if no per-partition stats were recorded, or if the per-partition
  // stats could not be deserialised from the parameter map.
  public TPartitionStats getPartitionStats() {
    try {
      return PartitionStatsUtil.partStatsFromParameters(hmsParameters_);
    } catch (ImpalaException e) {
      LOG.warn("Could not deserialise incremental stats state for " + getPartitionName() +
          ", consider DROP INCREMENTAL STATS ... PARTITION ... and recomputing " +
          "incremental stats for this table.");
      return null;
    }
  }

  public boolean hasIncrementalStats() {
    TPartitionStats partStats = getPartitionStats();
    return partStats != null && partStats.intermediate_col_stats != null;
  }

  // Returns the HDFS permissions Impala has to this partition's directory - READ_ONLY,
  // READ_WRITE, etc.
  public TAccessLevel getAccessLevel() { return accessLevel_; }

  /**
   * Returns the HMS parameter with key 'key' if it exists, otherwise returns null.
   */
   public String getHmsParameter(String key) {
     return hmsParameters_.get(key);
   }

  /**
   * Marks this partition's metadata as "dirty" indicating that changes have been
   * made and this partition's metadata should not be reused during the next
   * incremental metadata refresh.
   */
  public void markDirty() { isDirty_ = true; }
  public boolean isDirty() { return isDirty_; }

  /**
   * Returns an immutable list of partition key expressions
   */
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues_; }
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors_;
  }

  public boolean hasFileDescriptors() { return !fileDescriptors_.isEmpty(); }

  private HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
      String location, TAccessLevel accessLevel) {
    table_ = table;
    msPartition_ = msPartition;
    location_ = location;
    partitionKeyValues_ = ImmutableList.copyOf(partitionKeyValues);
    fileDescriptors_ = ImmutableList.copyOf(fileDescriptors);
    fileFormatDescriptor_ = fileFormatDescriptor;
    id_ = id;
    accessLevel_ = accessLevel;
    if (msPartition != null && msPartition.getParameters() != null) {
      isMarkedCached_ = HdfsCachingUtil.getCacheDirIdFromParams(
          msPartition.getParameters()) != null;
      hmsParameters_ = msPartition.getParameters();
    } else {
      hmsParameters_ = Maps.newHashMap();
    }

    // TODO: instead of raising an exception, we should consider marking this partition
    // invalid and moving on, so that table loading won't fail and user can query other
    // partitions.
    for (FileDescriptor fileDescriptor: fileDescriptors_) {
      StringBuilder errorMsg = new StringBuilder();
      if (!getInputFormatDescriptor().getFileFormat().isFileCompressionTypeSupported(
          fileDescriptor.getFileName(), errorMsg)) {
        throw new RuntimeException(errorMsg.toString());
      }
    }
  }

  public HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, TAccessLevel accessLevel) {
    this(table, msPartition, partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter_.getAndIncrement(), msPartition != null ?
            msPartition.getSd().getLocation() : table.getLocation(), accessLevel);
  }

  public static HdfsPartition defaultPartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    return new HdfsPartition(table, null, emptyExprList,
        storageDescriptor, emptyFileDescriptorList,
        ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID, null,
        TAccessLevel.READ_WRITE);
  }

  /**
   * Return the size (in bytes) of all the files inside this partition
   */
  public long getSize() {
    long result = 0;
    for (HdfsPartition.FileDescriptor fileDescriptor: fileDescriptors_) {
      result += fileDescriptor.getFileLength();
    }
    return result;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
<<<<<<< HEAD
      .add("fileDescriptors", fileDescriptors)
      .toString();
  }

  public THdfsPartition toThrift() {
    List<TExpr> thriftExprs =
      Expr.treesToThrift(getPartitionValues());

    return new THdfsPartition((byte)fileFormatDescriptor.getLineDelim(),
        (byte)fileFormatDescriptor.getFieldDelim(),
        (byte)fileFormatDescriptor.getCollectionDelim(),
        (byte)fileFormatDescriptor.getMapKeyDelim(),
        (byte)fileFormatDescriptor.getEscapeChar(),
        fileFormatDescriptor.getFileFormat().toThrift(), thriftExprs,
        fileFormatDescriptor.getBlockSize(), fileFormatDescriptor.getCompression());
  }
}
=======
      .add("fileDescriptors", fileDescriptors_)
      .toString();
  }

  public static HdfsPartition fromThrift(HdfsTable table,
      long id, THdfsPartition thriftPartition) {
    HdfsStorageDescriptor storageDesc = new HdfsStorageDescriptor(table.getName(),
        HdfsFileFormat.fromThrift(thriftPartition.getFileFormat()),
        thriftPartition.lineDelim,
        thriftPartition.fieldDelim,
        thriftPartition.collectionDelim,
        thriftPartition.mapKeyDelim,
        thriftPartition.escapeChar,
        (byte) '"', // TODO: We should probably add quoteChar to THdfsPartition.
        thriftPartition.blockSize);

    List<LiteralExpr> literalExpr = Lists.newArrayList();
    if (id != ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
      List<Column> clusterCols = Lists.newArrayList();
      for (int i = 0; i < table.getNumClusteringCols(); ++i) {
        clusterCols.add(table.getColumns().get(i));
      }

      List<TExprNode> exprNodes = Lists.newArrayList();
      for (TExpr expr: thriftPartition.getPartitionKeyExprs()) {
        for (TExprNode node: expr.getNodes()) {
          exprNodes.add(node);
        }
      }
      Preconditions.checkState(clusterCols.size() == exprNodes.size(),
          String.format("Number of partition columns (%d) does not match number " +
              "of partition key expressions (%d)",
              clusterCols.size(), exprNodes.size()));

      for (int i = 0; i < exprNodes.size(); ++i) {
        literalExpr.add(LiteralExpr.fromThrift(
            exprNodes.get(i), clusterCols.get(i).getType()));
      }
    }

    List<HdfsPartition.FileDescriptor> fileDescriptors = Lists.newArrayList();
    if (thriftPartition.isSetFile_desc()) {
      for (THdfsFileDesc desc: thriftPartition.getFile_desc()) {
        fileDescriptors.add(HdfsPartition.FileDescriptor.fromThrift(desc));
      }
    }

    TAccessLevel accessLevel = thriftPartition.isSetAccess_level() ?
        thriftPartition.getAccess_level() : TAccessLevel.READ_WRITE;
    HdfsPartition partition = new HdfsPartition(table, null, literalExpr, storageDesc,
        fileDescriptors, id, thriftPartition.getLocation(), accessLevel);
    if (thriftPartition.isSetStats()) {
      partition.setNumRows(thriftPartition.getStats().getNum_rows());
    }
    if (thriftPartition.isSetIs_marked_cached()) {
      partition.isMarkedCached_ = thriftPartition.isIs_marked_cached();
    }

    if (thriftPartition.isSetHms_parameters()) {
      partition.hmsParameters_ = thriftPartition.getHms_parameters();
    } else {
      partition.hmsParameters_ = Maps.newHashMap();
    }

    return partition;
  }

  /**
   * Checks that this partition's metadata is well formed. This does not necessarily
   * mean the partition is supported by Impala.
   * Throws a CatalogException if there are any errors in the partition metadata.
   */
  public void checkWellFormed() throws CatalogException {
    try {
      // Validate all the partition key/values to ensure you can convert them toThrift()
      Expr.treesToThrift(getPartitionValues());
    } catch (Exception e) {
      throw new CatalogException("Partition (" + getPartitionName() +
          ") has invalid partition column values: ", e);
    }
  }

  public THdfsPartition toThrift(boolean includeFileDesc) {
    List<TExpr> thriftExprs = Expr.treesToThrift(getPartitionValues());

    THdfsPartition thriftHdfsPart = new THdfsPartition(
        fileFormatDescriptor_.getLineDelim(),
        fileFormatDescriptor_.getFieldDelim(),
        fileFormatDescriptor_.getCollectionDelim(),
        fileFormatDescriptor_.getMapKeyDelim(),
        fileFormatDescriptor_.getEscapeChar(),
        fileFormatDescriptor_.getFileFormat().toThrift(), thriftExprs,
        fileFormatDescriptor_.getBlockSize());
    thriftHdfsPart.setLocation(location_);
    thriftHdfsPart.setStats(new TTableStats(numRows_));
    thriftHdfsPart.setAccess_level(accessLevel_);
    thriftHdfsPart.setIs_marked_cached(isMarkedCached_);
    thriftHdfsPart.setId(getId());
    thriftHdfsPart.setHms_parameters(hmsParameters_);
    if (includeFileDesc) {
      // Add block location information
      for (FileDescriptor fd: fileDescriptors_) {
        thriftHdfsPart.addToFile_desc(fd.toThrift());
      }
    }

    return thriftHdfsPart;
  }

  /**
   * Comparison method to allow ordering of HdfsPartitions by their partition-key values.
   */
  @Override
  public int compareTo(HdfsPartition o) {
    int sizeDiff = partitionKeyValues_.size() - o.getPartitionValues().size();
    if (sizeDiff != 0) return sizeDiff;
    for (int i = 0; i < partitionKeyValues_.size(); ++i) {
      int cmp = partitionKeyValues_.get(i).compareTo(o.getPartitionValues().get(i));
      if (cmp != 0) return cmp;
    }
    return 0;
  }
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
