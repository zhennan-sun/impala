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

namespace cpp impala
namespace java com.cloudera.impala.thrift

<<<<<<< HEAD
=======
include "CatalogObjects.thrift"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
include "Types.thrift"
include "Exprs.thrift"

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
<<<<<<< HEAD
  3: required Types.TPrimitiveType slotType
=======
  3: required Types.TColumnType slotType
  // TODO: Make this a list of positions to support SlotRefs into struct fields.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  4: required i32 columnPos   // in originating table
  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  9: required i32 slotIdx
  10: required bool isMaterialized
}

<<<<<<< HEAD
enum TTableType {
  HDFS_TABLE,
  HBASE_TABLE
}

enum THdfsFileFormat {
  TEXT,
  RC_FILE,
  SEQUENCE_FILE,
  TREVNI
}

enum THdfsCompression {
  NONE,
  DEFAULT,
  GZIP,
  BZIP2,
  SNAPPY,
  SNAPPY_BLOCKED // Used by sequence and rc files but not stored in the metadata.
}

struct THdfsPartition {
  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: required THdfsFileFormat fileFormat
  7: list<Exprs.TExpr> partitionKeyExprs 
  8: required i32 blockSize
  9: required THdfsCompression compression
}

struct THdfsTable {
  1: required string hdfsBaseDir

  // Partition keys are the same as clustering columns in
  // TTableDescriptor, so there should be an equal number of each.
  2: required list<string> partitionKeyNames
  3: required string nullPartitionKeyValue

  // map from partition id to partition metadata
  4: required map<i64, THdfsPartition> partitions
}

struct THBaseTable {
  1: required string tableName
  2: required list<string> families
  3: required list<string> qualifiers
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols
  5: optional THdfsTable hdfsTable
  6: optional THBaseTable hbaseTable
=======
// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required CatalogObjects.TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols
  // Names of the columns. Clustering columns come first.
  10: optional list<string> colNames;
  5: optional CatalogObjects.THdfsTable hdfsTable
  6: optional CatalogObjects.THBaseTable hbaseTable
  9: optional CatalogObjects.TDataSourceTable dataSourceTable
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Unqualified name of table
  7: required string tableName;

  // Name of the database that the table belongs to
  8: required string dbName;
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
}
