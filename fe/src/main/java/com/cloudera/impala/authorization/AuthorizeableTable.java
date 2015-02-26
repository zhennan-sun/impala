// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.authorization;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class used to authorize access to a table or view.
 * Even though Hive's spec includes an authorizable object 'view', we chose
 * to treat views the same way as tables for the sake of authorization.
 */
public class AuthorizeableTable implements Authorizeable {
  // Constant to represent privileges in the policy for "ANY" table in a
  // a database.
  public final static String ANY_TABLE_NAME = org.apache.sentry.core.AccessConstants.ALL;

  private final org.apache.sentry.core.Table table_;
  private final org.apache.sentry.core.Database database_;

  public AuthorizeableTable(String dbName, String tableName) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    Preconditions.checkState(dbName != null && !dbName.isEmpty());
    table_ = new org.apache.sentry.core.Table(tableName);
    database_ = new org.apache.sentry.core.Database(dbName);
  }

  @Override
  public List<org.apache.sentry.core.Authorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList(database_, table_);
  }

  @Override
  public String getName() { return database_.getName() + "." + table_.getName(); }
  public String getDbName() { return database_.getName(); }
  public String getTblName() { return table_.getName(); }
}
