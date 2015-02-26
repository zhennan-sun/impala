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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.planner.DataSink;
=======
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.cloudera.impala.thrift.TCatalogObjectType;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.cloudera.impala.thrift.TTableDescriptor;
import com.google.common.base.Preconditions;

/**
<<<<<<< HEAD
 * A fake catalog representation of an inline view. It's like a table. It has name
 * and columns, but it won't have ids and it shouldn't be converted to Thrift.
 * An inline view is constructed by providing an alias (required) and then adding columns
 * one-by-one.
=======
 * A fake catalog representation of an inline view that is similar to a table.
 * Inline views originating directly from the query or from a local view can only
 * be referenced by their explicit alias, so their table name is the alias. Inline views
 * instantiated from a catalog view have a db and a table name such that we can generate
 * unqualified and fully-qualified implicit aliases.
 * Fake tables for inline views do not have an id, msTbl or an owner and cannot be
 * converted to Thrift.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
 */
public class InlineView extends Table {

  /**
<<<<<<< HEAD
   * An inline view only has an alias and columns, but it won't have id, db and owner.
   * @param alias alias of the inline view; must not be null;
   */
  public InlineView(String alias) {
    super(null, null, alias, null);
=======
   * C'tor for inline views that only have an explicit alias and not a real table name.
   */
  public InlineView(String alias) {
    super(null, null, null, alias, null);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    Preconditions.checkArgument(alias != null);
  }

  /**
<<<<<<< HEAD
   * An inline view has to be constructed by adding columns one by one.
   * @param col Column of the inline view
   */
  public void addColumn(Column col) {
    colsByPos.add(col);
    colsByName.put(col.getName(), col);
=======
   * C'tor for inline views instantiated from a local or catalog view.
   */
  public InlineView(Table tbl) {
    super(null, null, tbl.getDb(), tbl.getName(), null);
    Preconditions.checkArgument(tbl != null);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  /**
   * This should never be called.
   */
  @Override
<<<<<<< HEAD
  public Table load(HiveMetaStoreClient client,
=======
  public void load(Table oldValue, HiveMetaStoreClient client,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // An inline view is never loaded
    throw new UnsupportedOperationException("Inline View should never be loaded");
  }

<<<<<<< HEAD
=======
  @Override
  public boolean isVirtualTable() { return true; }
  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.VIEW; }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  /**
   * This should never be called.
   */
  @Override
<<<<<<< HEAD
  public TTableDescriptor toThrift() {
    // An inline view never generate Thrift representation.
=======
  public TTableDescriptor toThriftDescriptor(Set<Long> referencedPartitions) {
    // An inline view never generates Thrift representation.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    throw new UnsupportedOperationException(
        "Inline View should not generate Thrift representation");
  }

<<<<<<< HEAD
  /**
   * This should never be called.
   */
  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    // An inlinview should never be a target of a n insert.
    throw new UnsupportedOperationException(
        "Inline View should never be the target of an insert");
  }

}
=======
  @Override
  public int getNumNodes() {
    throw new UnsupportedOperationException("InlineView.getNumNodes() not supported");
  }
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
