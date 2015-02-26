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
package org.apache.sentry.binding.hive;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.AccessConstants;
import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.sentry.core.Server;
import org.apache.sentry.core.Subject;
import org.apache.sentry.core.Table;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test for hive authz bindings
 * It uses the access.provider.file.ResourceAuthorizationProvider with the
 * resource test-authz-provider.ini
 */
public class TestHiveAuthzBindings {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  // Servers
  private static final String SERVER1 = "server1";

  // Users
  private static final Subject ADMIN_SUBJECT = new Subject("admin1");
  private static final Subject MANAGER_SUBJECT = new Subject("manager1");
  private static final Subject ANALYST_SUBJECT = new Subject("analyst1");
  private static final Subject JUNIOR_ANALYST_SUBJECT = new Subject("junior_analyst1");
  private static final Subject NO_SUCH_SUBJECT = new Subject("no such subject");

  // Databases
  private static final String CUSTOMER_DB = "customers";
  private static final String ANALYST_DB = "analyst";
  private static final String JUNIOR_ANALYST_DB = "junior_analyst";

  // Tables
  private static final String PURCHASES_TAB = "purchases";
  private static final String PAYMENT_TAB = "payments";

  // Entities
  private List<List<Authorizable>> inputTabHierarcyList = new ArrayList<List<Authorizable>>();
  private List<List<Authorizable>> outputTabHierarcyList = new ArrayList<List<Authorizable>>();
  private HiveConf hiveConf = new HiveConf();
  private HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("access-site.xml"));

  // Privileges
  private static final HiveAuthzPrivileges queryPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.QUERY);
  private static final HiveAuthzPrivileges createTabPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATETABLE);
  private static final HiveAuthzPrivileges loadTabPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.LOAD);
  private static final HiveAuthzPrivileges createDbPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATEDATABASE);
  private static final HiveAuthzPrivileges createFuncPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATEFUNCTION);

  // auth bindings handler
  private HiveAuthzBinding testAuth = null;
  private File baseDir;

  @Before
  public void setUp() throws Exception {
    inputTabHierarcyList.clear();
    outputTabHierarcyList.clear();
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
        new File(baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), SERVER1);
    authzConf.set(AuthzConfVars.ACCESS_TESTING_MODE.getVar(), "true");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForAdmin() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForUsers() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate read permission for denied for junior analyst on customer:purchase
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateSelectPrivilegesRejectionForUsers() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions for admin in customer db
   */
  @Test
  public void testValidateCreateTabPrivilegesForAdmin() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PAYMENT_TAB));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions for manager in junior_analyst sandbox db
   */
  @Test
  public void testValidateCreateTabPrivilegesForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, JUNIOR_ANALYST_DB, PAYMENT_TAB));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, MANAGER_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions denided to junior_analyst in customer db
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions denided to junior_analyst in analyst sandbox db
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser2() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, ANALYST_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate load permissions for admin on customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForAdmin() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate load table permissions on manager for customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, MANAGER_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);  }

  /**
   * validate load table permissions rejected for analyst on customer:purchases
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateLoadTabPrivilegesRejectionForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create database permission for admin
   */
  @Test
  public void testValidateCreateDbForAdmin() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, null, null));
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create database permission for admin
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateDbRejectionForUser() throws Exception {
    // Hive compiler doesn't capture Entities for DB operations
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, null, null));
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Validate create function permission for admin (server level priviledge
   */
  @Test
  public void testValidateCreateFunctionForAdmin() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, null, null));
    inputTabHierarcyList.add(Arrays.asList(new Authorizable[] {
        new Server(SERVER1), new AccessURI("file:///some/path/to/a/jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }
  @Test
  public void testValidateCreateFunctionAppropiateURI() throws Exception {
    inputTabHierarcyList.add(Arrays.asList(new Authorizable[] {
        new Server(SERVER1), new Database(CUSTOMER_DB), new Table(AccessConstants.ALL)
    }));
    inputTabHierarcyList.add(Arrays.asList(new Authorizable[] {
        new Server(SERVER1), new AccessURI("file:///path/to/some/lib/dir/my.jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateFunctionRejectionForUnknownUser() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, AccessConstants.ALL));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, NO_SUCH_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateFunctionRejectionForUserWithoutURI() throws Exception {
    inputTabHierarcyList.add(Arrays.asList(new Authorizable[] {
        new Server(SERVER1), new Database(CUSTOMER_DB), new Table(AccessConstants.ALL)
    }));
    inputTabHierarcyList.add(Arrays.asList(new Authorizable[] {
        new Server(SERVER1), new AccessURI("file:///some/path/to/a.jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Turn on impersonation and make sure that the authorization fails.
   * @throws Exception
   */
  @Test(expected=AuthorizationException.class)
  public void testImpersonationRestriction() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_KERBEROS_IMPERSONATION, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.ACCESS_TESTING_MODE.getVar(), "false");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);

    // following check should pass, but with impersonation it will fail with due to NoAuthorizationProvider
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Turn on impersonation and make sure that the authorization fails.
   * @throws Exception
   */
  @Test
  public void testImpersonationAllowed() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_KERBEROS_IMPERSONATION, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.ACCESS_TESTING_MODE.getVar(), "false");
    authzConf.set(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar(), "true");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);

    // following check should pass, even with impersonation
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  private List <Authorizable>  buildObjectHierarchy(String server, String db, String table) {
    List <Authorizable> authList = new ArrayList<Authorizable> ();
    authList.add(new Server(server));
    if (db != null) {
      authList.add(new Database(db));
      if (table != null) {
        authList.add(new Table(table));
      }
    }
    return authList;
  }
}
