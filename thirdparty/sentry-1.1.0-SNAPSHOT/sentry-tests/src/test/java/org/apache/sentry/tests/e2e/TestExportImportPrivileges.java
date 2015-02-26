/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestExportImportPrivileges extends AbstractTestWithStaticDFS {
  private File dataFile;

  @Before
  public void setup() throws Exception {
    context = createContext();
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

  @Test
  public void testInsertToDirPrivileges() throws Exception {
    Connection connection = null;
    Statement statement = null;
    String dumpDir = context.getDFSUri().toString() + "/hive_data_dump";

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = db1_read, db1_write, data_dump",
        "user_group2  = db1_read, db1_write",
        "[roles]",
        "db1_write = server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=INSERT",
        "db1_read = server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=SELECT",
        "data_dump = server=server1->URI=" + dumpDir,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        ADMIN1 + " = admin_group"
    };
    context.makeNewPolicy(testPolicies);

    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);

    // Negative test, user2 doesn't have access to write to dir
    connection = context.createConnection(USER2, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    context.assertAuthzException(statement, "INSERT OVERWRITE DIRECTORY '" + dumpDir + "' SELECT * FROM " + TBL1);
    statement.close();
    connection.close();

    // positive test, user1 has access to write to dir
    connection = context.createConnection(USER1, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    assertTrue(statement.executeQuery("SELECT * FROM " + TBL1).next());
    statement.execute("INSERT OVERWRITE DIRECTORY '" + dumpDir + "' SELECT * FROM " + TBL1);
  }

  @Test
  public void testExportImportPrivileges() throws Exception {
    Connection connection = null;
    Statement statement = null;
    String exportDir = context.getDFSUri().toString() + "/hive_export1";

    String testPolicies[] = {
        "[groups]",
        "admin_group = admin_role",
        "user_group1  = tab1_read, tab1_write, db1_all, data_read, data_export",
        "user_group2  = tab1_write, tab1_read",
        "[roles]",
        "tab1_write = server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=INSERT",
        "tab1_read = server=server1->db=" + DB1 + "->table=" + TBL1 + "->action=SELECT",
        "db1_all = server=server1->db=" + DB1,
        "data_read = server=server1->URI=file://" + dataFile.getPath(),
        "data_export = server=server1->URI=" + exportDir,
        "admin_role = server=server1",
        "[users]",
        "user1 = user_group1",
        "user2 = user_group2",
        ADMIN1 + " = admin_group"
    };
    context.makeNewPolicy(testPolicies);

    dropDb(ADMIN1, DB1);
    createDb(ADMIN1, DB1);
    createTable(ADMIN1, DB1, dataFile, TBL1);

    // Negative test, user2 doesn't have access to the file being loaded
    connection = context.createConnection(USER2, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    context.assertAuthzException(statement, "EXPORT TABLE " + TBL1 + " TO '" + exportDir + "'");
    statement.close();
    connection.close();

    // Positive test, user1 have access to the target directory
    connection = context.createConnection(USER1, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("EXPORT TABLE " + TBL1 + " TO '" + exportDir + "'");
    statement.close();
    connection.close();

    // Negative test, user2 doesn't have access to the directory loading from
    connection = context.createConnection(USER2, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    context.assertAuthzException(statement, "IMPORT TABLE " + TBL2 + " FROM '" + exportDir + "'");
    statement.close();
    connection.close();

    // Positive test, user1 have access to the target directory
    connection = context.createConnection(USER1, "password");
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("IMPORT TABLE " + TBL2 + " FROM '" + exportDir + "'");
    statement.close();
    connection.close();
  }
}
