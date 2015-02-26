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

=======
import com.cloudera.impala.catalog.Table;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
<<<<<<< HEAD
 * An actual table, such as HBase table or a Hive table.
 * BaseTableRef.
 */
public class BaseTableRef extends TableRef {
  private TableName name;

  public BaseTableRef(TableName name, String alias) {
    super(alias);
    Preconditions.checkArgument(!name.toString().isEmpty());
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.name = name;
  }


  /**
   * Returns the name of the table referred to. Before analysis, the table name
   * may not be fully qualified; afterwards it is guaranteed to be fully
   * qualified.
   */
  public TableName getName() {
    return name;
=======
 * Represents a reference to an actual table, such as an Hdfs or HBase table.
 * BaseTableRefs are instantiated as a result of table resolution during analysis
 * of a SelectStmt.
 */
public class BaseTableRef extends TableRef {
  private final Table table_;

  public BaseTableRef(TableRef tableRef, Table table) {
    super(tableRef);
    table_ = table;
  }

  /**
   * C'tor for cloning.
   */
  private BaseTableRef(BaseTableRef other) {
    super(other);
    table_ = other.table_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
<<<<<<< HEAD
    desc = analyzer.registerBaseTableRef(this);
    analyzeJoin(analyzer);
    isAnalyzed = true;
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    // This function should only be called after analyze().
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkState(desc != null);
    return desc.getId().asList();
  }

  /**
   * Return alias by which this table is referenced in select block.
   */
  @Override
  public String getAlias() {
    if (alias == null) {
      return name.toString().toLowerCase();
    } else {
      return alias;
    }
  }

  @Override
  public TableName getAliasAsName() {
    if (alias != null) {
      return new TableName(null, alias);
    } else {
      return name;
    }
  }

  @Override
  protected String tableRefToSql() {
    return name.toString() + (alias != null ? " " + alias : "");
  }
=======
    Preconditions.checkNotNull(getPrivilegeRequirement());
    setFullyQualifiedTableName(analyzer);
    desc_ = analyzer.registerTableRef(this);
    isAnalyzed_ = true;  // true that we have assigned desc
    analyzeJoin(analyzer);
  }

  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor("basetbl");
    result.setTable(table_);
    return result;
  }

  @Override
  public TableRef clone() { return new BaseTableRef(this); }
  public String debugString() { return tableRefToSql(); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
