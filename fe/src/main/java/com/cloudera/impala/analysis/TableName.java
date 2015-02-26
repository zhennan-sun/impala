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
import com.google.common.base.Preconditions;

public class TableName {
  private final String db;
  private final String tbl;
=======
import org.apache.hadoop.hive.metastore.MetaStoreUtils;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Represents a table/view name that optionally includes its database (a fully qualified
 * table name). Analysis of this table name checks for validity of the database and
 * table name according to the Metastore's policy (see @MetaStoreUtils).
 * According to that definition, we can still use "invalid" table names for tables/views
 * that are not stored in the Metastore, e.g., for Inline Views or WITH-clause views.
 */
public class TableName {
  private final String db_;
  private final String tbl_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  public TableName(String db, String tbl) {
    super();
    Preconditions.checkArgument(db == null || !db.isEmpty());
<<<<<<< HEAD
    this.db = db;
    Preconditions.checkNotNull(tbl);
    this.tbl = tbl;
  }

  public String getDb() {
    return db;
  }

  public String getTbl() {
    return tbl;
  }

  public boolean isEmpty() {
    return tbl.isEmpty();
  }

  /**
   * Returns true if this name has a non-empty database field
   */
  public boolean isFullyQualified() {
    return db != null && !db.isEmpty();
=======
    this.db_ = db;
    Preconditions.checkNotNull(tbl);
    this.tbl_ = tbl;
  }

  public String getDb() { return db_; }
  public String getTbl() { return tbl_; }
  public boolean isEmpty() { return tbl_.isEmpty(); }

  /**
   * Checks whether the db and table name meet the Metastore's requirements.
   */
  public void analyze() throws AnalysisException {
    if (db_ != null) {
      if (!MetaStoreUtils.validateName(db_)) {
        throw new AnalysisException("Invalid database name: " + db_);
      }
    }
    Preconditions.checkNotNull(tbl_);
    if (!MetaStoreUtils.validateName(tbl_)) {
      throw new AnalysisException("Invalid table/view name: " + tbl_);
    }
  }

  /**
   * Returns true if this name has a non-empty database field and a non-empty
   * table name.
   */
  public boolean isFullyQualified() {
    return db_ != null && !db_.isEmpty() && !tbl_.isEmpty();
  }

  public String toSql() {
    // Enclose the database and/or table name in quotes if Hive cannot parse them
    // without quotes. This is needed for view compatibility between Impala and Hive.
    if (db_ == null) {
      return ToSqlUtils.getIdentSql(tbl_);
    } else {
      return ToSqlUtils.getIdentSql(db_) + "." + ToSqlUtils.getIdentSql(tbl_);
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  @Override
  public String toString() {
<<<<<<< HEAD
    if (db == null) {
      return tbl;
    } else {
      return db + "." + tbl;
    }
  }
}
=======
    if (db_ == null) {
      return tbl_;
    } else {
      return db_ + "." + tbl_;
    }
  }

  public static TableName fromThrift(TTableName tableName) {
    return new TableName(tableName.getDb_name(), tableName.getTable_name());
  }

  public TTableName toThrift() { return new TTableName(db_, tbl_); }

  /**
   * Returns true of the table names are considered equals. To check for equality,
   * a case-insensitive comparison of the database and table name is performed.
   */
  @Override
  public boolean equals(Object anObject) {
    if (anObject instanceof TableName) {
      return toString().toLowerCase().equals(anObject.toString().toLowerCase());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toString().toLowerCase().hashCode();
  }
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
