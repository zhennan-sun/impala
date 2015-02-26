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
package org.apache.sentry.provider.file;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.io.Resources;

public class Roles {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(Roles.class);
  private final ImmutableSetMultimap<String, String> globalRoles;
  private final ImmutableMap<String, ImmutableSetMultimap<String, String>> perDatabaseRoles;
  public Roles() {
    this(ImmutableSetMultimap.<String,String>of(),
        ImmutableMap.<String, ImmutableSetMultimap<String, String>>of());
  }
  public Roles(
      ImmutableSetMultimap<String, String> globalRoles,
      ImmutableMap<String, ImmutableSetMultimap<String, String>> perDatabaseRoles) {
    super();
    this.globalRoles = globalRoles;
    this.perDatabaseRoles = perDatabaseRoles;
  }
  public ImmutableSet<String> getRoles(@Nullable String database, String group, Boolean isURI) {
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
    String allowURIPerDbFile = 
        System.getProperty(SimplePolicyEngine.ACCESS_ALLOW_URI_PER_DB_POLICYFILE);
    Boolean consultPerDbRolesForURI = isURI && ("true".equalsIgnoreCase(allowURIPerDbFile));

    if(database != null) {
      ImmutableSetMultimap<String, String> dbPolicies =  perDatabaseRoles.get(database);
      if(dbPolicies != null && dbPolicies.containsKey(group)) {
        resultBuilder.addAll(dbPolicies.get(group));
      }
    }
    if (consultPerDbRolesForURI) {
      for(String db:perDatabaseRoles.keySet()) {
        ImmutableSetMultimap<String, String> dbPolicies =  perDatabaseRoles.get(db);
        if(dbPolicies != null && dbPolicies.containsKey(group)) {
          resultBuilder.addAll(dbPolicies.get(group));
        }
      }
    }

    if(globalRoles.containsKey(group)) {
      resultBuilder.addAll(globalRoles.get(group));
    }
    ImmutableSet<String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Database {}, Group {}, Result {}",
          new Object[]{ database, group, result});
    }
    return result;
  }
}
