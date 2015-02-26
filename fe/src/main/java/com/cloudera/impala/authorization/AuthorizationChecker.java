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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.provider.file.ResourceAuthorizationProvider;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.AuthorizationPolicy;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*
 * Class used to check whether a user has access to a given resource.
 */
public class AuthorizationChecker {
  private final ResourceAuthorizationProvider provider_;
  private final AuthorizationConfig config_;
  private final AuthorizeableServer server_;

  /*
   * Creates a new AuthorizationChecker based on the config values.
   */
  public AuthorizationChecker(AuthorizationConfig config, AuthorizationPolicy policy) {
    Preconditions.checkNotNull(config);
    config_ = config;
    if (config.isEnabled()) {
      server_ = new AuthorizeableServer(config.getServerName());
      provider_ = createProvider(config, policy);
      Preconditions.checkNotNull(provider_);
    } else {
      provider_ = null;
      server_ = null;
    }
  }

  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  private static ResourceAuthorizationProvider createProvider(AuthorizationConfig config,
      AuthorizationPolicy policy) {
    try {
      // Try to create an instance of the specified policy provider class.
      // Re-throw any exceptions that are encountered.
      return (ResourceAuthorizationProvider) ConstructorUtils.invokeConstructor(
          Class.forName(config.getPolicyProviderClassName()),
          new Object[] {config.getPolicyFile(), config.getServerName()});
    } catch (Exception e) {
      // Re-throw as unchecked exception.
      throw new IllegalStateException(
          "Error creating ResourceAuthorizationProvider: " + e.getMessage(), e);
    }
  }

  /*
   * Returns the configuration used to create this AuthorizationProvider.
   */
  public AuthorizationConfig getConfig() { return config_; }

  /**
   * Returns the set of groups this user belongs to. Uses the GroupMappingService
   * that is in the AuthorizationProvider to properly resolve Hadoop groups or
   * local group mappings.
   */
  public Set<String> getUserGroups(User user) {
    throw new UnsupportedOperationException("This API is not supported on CDH4.");
  }

  /**
   * Authorizes the PrivilegeRequest, throwing an Authorization exception if
   * the user does not have sufficient privileges.
   */
  public void checkAccess(User user, PrivilegeRequest privilegeRequest)
      throws AuthorizationException {
    Preconditions.checkNotNull(privilegeRequest);

    if (!hasAccess(user, privilegeRequest)) {
      if (privilegeRequest.getAuthorizeable() instanceof AuthorizeableFn) {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to CREATE/DROP functions.",
            user.getName()));
      }

      Privilege privilege = privilegeRequest.getPrivilege();
      if (EnumSet.of(Privilege.ANY, Privilege.ALL, Privilege.VIEW_METADATA)
          .contains(privilege)) {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to access: %s",
            user.getName(), privilegeRequest.getName()));
      } else {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to execute '%s' on: %s",
            user.getName(), privilege, privilegeRequest.getName()));
      }
    }
  }

  /*
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled.
   */
  public boolean hasAccess(User user, PrivilegeRequest request) {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);

    // If authorization is not enabled the user will always have access. If this is
    // an internal request, the user will always have permission.
    if (!config_.isEnabled() || user instanceof ImpalaInternalAdminUser) {
      return true;
    }

    EnumSet<org.apache.sentry.core.Action> actions =
        request.getPrivilege().getHiveActions();

    List<Authorizable> authorizeables = Lists.newArrayList();
    authorizeables.add(new org.apache.sentry.core.Server(config_.getServerName()));

    // If request.getAuthorizeable() is null, the request is for server-level permission.
    if (request.getAuthorizeable() != null) {
      authorizeables.addAll(request.getAuthorizeable().getHiveAuthorizeableHierarchy());
    }

    // The Hive Access API does not currently provide a way to check if the user
    // has any privileges on a given resource.
    if (request.getPrivilege().getAnyOf()) {
      for (org.apache.sentry.core.Action action: actions) {
        if (provider_.hasAccess(new org.apache.sentry.core.Subject(user.getShortName()),
            authorizeables, EnumSet.of(action))) {
          return true;
        }
      }
      return false;
    } else if (request.getPrivilege() == Privilege.CREATE && authorizeables.size() > 1) {
      // CREATE on an object requires CREATE on the parent,
      // so don't check access on the object we're creating.
      authorizeables.remove(authorizeables.size() - 1);
    }
    return provider_.hasAccess(new org.apache.sentry.core.Subject(user.getShortName()),
        authorizeables, actions);
  }
}
