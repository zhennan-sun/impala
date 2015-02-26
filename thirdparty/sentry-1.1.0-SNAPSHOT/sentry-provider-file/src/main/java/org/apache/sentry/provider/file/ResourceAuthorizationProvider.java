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

import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.PRIVILEGE_NAME;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.sentry.core.Action;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.AuthorizationProvider;
import org.apache.sentry.core.Database;
import org.apache.sentry.core.Server;
import org.apache.sentry.core.ServerResource;
import org.apache.sentry.core.Subject;
import org.apache.sentry.core.Table;
import org.apache.shiro.authz.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public abstract class ResourceAuthorizationProvider implements AuthorizationProvider {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ResourceAuthorizationProvider.class);
  private final GroupMappingService groupService;
  private final PolicyEngine policy;

  public ResourceAuthorizationProvider(PolicyEngine policy,
      GroupMappingService groupService) {
    this.policy = policy;
    this.groupService = groupService;
  }

  @Override
  public boolean hasAccess(Subject subject, Server server, Database database,
      Table table, EnumSet<Action> actions) {
    List<Authorizable> authorizables = Lists.newArrayList();
    authorizables.add(server);
    authorizables.add(database);
    authorizables.add(table);
    return hasAccess(subject, authorizables, actions);
  }

  @Override
  public boolean hasAccess(Subject subject, Server server,
      ServerResource serverResource, EnumSet<Action> actions) {
    throw new UnsupportedOperationException("Deprecated");
  }

  /***
   * @param subject: UserID to validate privileges
   * @param authorizableHierarchy : List of object according to namespace hierarchy.
   *        eg. Server->Db->Table or Server->Function
   *        The privileges will be validated from the higher to lower scope
   * @param actions : Privileges to validate
   * @return
   *        True if the subject is authorized to perform requested action on the given object
   */
  @Override
  public boolean hasAccess(Subject subject, List<Authorizable> authorizableHierarchy,
      EnumSet<Action> actions) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Authorization Request for " + subject + " " +
          authorizableHierarchy + " and " + actions);
    }
    Preconditions.checkNotNull(subject, "Subject cannot be null");
    Preconditions.checkNotNull(authorizableHierarchy, "Authorizable cannot be null");
    Preconditions.checkArgument(!authorizableHierarchy.isEmpty(), "Authorizable cannot be empty");
    Preconditions.checkNotNull(actions, "Actions cannot be null");
    Preconditions.checkNotNull(!actions.isEmpty(), "Actions cannot be empty");
    return doHasAccess(subject, authorizableHierarchy, actions);
  }

  private boolean doHasAccess(Subject subject,
      List<Authorizable> authorizables, EnumSet<Action> actions) {
    List<String> groups = groupService.getGroups(subject.getName());
    List<String> hierarchy = new ArrayList<String>();
    for (Authorizable authorizable : authorizables) {
      hierarchy.add(KV_JOINER.join(authorizable.getAuthzType().name(), authorizable.getName()));
    }
    Iterable<Permission> permissions = getPermissions(authorizables, groups);
    for (Action action : actions) {
      String requestPermission = AUTHORIZABLE_JOINER.join(hierarchy);
      requestPermission = AUTHORIZABLE_JOINER.join(requestPermission,
          KV_JOINER.join(PRIVILEGE_NAME, action.getValue()));
      for (Permission permission : permissions) {
        /*
         * Does the permission granted in the policy file imply the requested action?
         */
        boolean result = permission.implies(new WildcardPermission(requestPermission));
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("FilePermission {}, RequestPermission {}, result {}",
              new Object[]{ permission, requestPermission, result});
        }
        if (result) {
          return true;
        }
      }
    }
    return false;
  }

  private Iterable<Permission> getPermissions(List<Authorizable> authorizables, List<String> groups) {
    return Iterables.transform(policy.getPermissions(authorizables, groups).values(),
        new Function<String, Permission>() {
      @Override
      public Permission apply(String permission) {
        return new WildcardPermission(permission);
      }
    });
  }
}
