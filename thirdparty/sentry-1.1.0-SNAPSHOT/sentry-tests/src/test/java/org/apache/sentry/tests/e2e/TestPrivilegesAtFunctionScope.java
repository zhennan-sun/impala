/*
printf_test_3 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestPrivilegesAtFunctionScope extends AbstractTestWithStaticLocalFS {
  private Context context;
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;

  @Before
  public void setup() throws Exception {
    context = createContext();
    dataDir = context.getDataDir();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /**
   * admin should be able to create/drop temp functions
   * user with db level access should be able to create/drop temp functions
   * user with table level access should be able to create/drop temp functions
   * user with no privilege should NOT be able to create/drop temp functions
   */
  @Test
  public void testFuncPrivileges1() throws Exception {
    String dbName1 = "db_1";
    String tableName1 = "tb_1";
    // edit policy file
    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = db1_all,UDF_JAR", "groups");
    editor.addPolicy("group2 = db1_tab1,UDF_JAR", "groups");
    editor.addPolicy("group3 = db1_tab1", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("db1_all = server=server1->db=" + dbName1, "roles");
    editor.addPolicy("db1_tab1 = server=server1->db=" + dbName1 + "->table=" + tableName1, "roles");
    editor.addPolicy("UDF_JAR = server=server1->uri=file://${user.home}/.m2", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");
    editor.addPolicy("user2 = group2", "users");
    editor.addPolicy("user3 = group3", "users");

    Connection connection = context.createConnection("admin1", "foo");
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("DROP TABLE IF EXISTS " + dbName1 + "." + tableName1);
    statement.execute("create table " + dbName1 + "." + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA INPATH '" + dataFile.getPath() + "' INTO TABLE "
        + dbName1 + "." + tableName1);
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test");
    statement.execute("DROP TEMPORARY FUNCTION IF EXISTS printf_test_2");
    context.close();

    // user1 should be able create/drop temp functions
    connection = context.createConnection("user1", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    statement.execute(
        "CREATE TEMPORARY FUNCTION printf_test AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
    statement.execute("DROP TEMPORARY FUNCTION printf_test");
    context.close();

    // user2 has select privilege on one of the tables in db2, should be able create/drop temp functions
    connection = context.createConnection("user2", "foo");
    statement = context.createStatement(connection);
    statement.execute("USE " + dbName1);
    statement.execute(
        "CREATE TEMPORARY FUNCTION printf_test_2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
    statement.execute("DROP TEMPORARY FUNCTION printf_test");
    context.close();

    // user3 shouldn't be able to create/drop temp functions since it doesn't have permission for jar
    connection = context.createConnection("user3", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE " + dbName1);
      statement.execute(
      "CREATE TEMPORARY FUNCTION printf_test_bad AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user3", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();

    // user4 (not part of any group ) shouldn't be able to create/drop temp functions
    connection = context.createConnection("user4", "foo");
    statement = context.createStatement(connection);
    try {
      statement.execute("USE default");
      statement.execute(
      "CREATE TEMPORARY FUNCTION printf_test_bad AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
      assertFalse("CREATE TEMPORARY FUNCTION should fail for user4", true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();

  }

  @Test
  public void testUdfWhiteList () throws Exception {
    String dbName1 = "db1";
    String tableName1 = "tab1";

    File policyFile = context.getPolicyFile();
    PolicyFileEditor editor = new PolicyFileEditor(policyFile);
    editor.addPolicy("admin = admin", "groups");
    editor.addPolicy("group1 = db1_all,UDF_JAR", "groups");
    editor.addPolicy("group2 = db1_tab1,UDF_JAR", "groups");
    editor.addPolicy("group3 = db1_tab1", "groups");
    editor.addPolicy("admin = server=server1", "roles");
    editor.addPolicy("db1_all = server=server1->db=" + dbName1, "roles");
    editor.addPolicy("db1_tab1 = server=server1->db=" + dbName1 + "->table=" + tableName1, "roles");
    editor.addPolicy("UDF_JAR = server=server1->uri=file://${user.home}/.m2", "roles");
    editor.addPolicy("admin1 = admin", "users");
    editor.addPolicy("user1 = group1", "users");

    Connection connection = context.createConnection("admin1", "password");
    Statement statement = connection.createStatement();
    statement.execute("DROP DATABASE IF EXISTS " + dbName1 + " CASCADE");
    statement.execute("CREATE DATABASE " + dbName1);
    statement.execute("USE " + dbName1);
    statement.execute("create table " + tableName1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("LOAD DATA INPATH '" + dataFile.getPath() + "' INTO TABLE "
        + dbName1 + "." + tableName1);
    statement.execute("SELECT rand(), concat(value, '_foo') FROM " + tableName1);

    context.assertAuthzException(statement,
        "SELECT  reflect('java.net.URLDecoder', 'decode', 'http://www.apache.org', 'utf-8'), value FROM " + tableName1);
    context.assertAuthzException(statement,
        "SELECT  java_method('java.net.URLDecoder', 'decode', 'http://www.apache.org', 'utf-8'), value FROM " + tableName1);
    statement.close();
    connection.close();
  }
}
