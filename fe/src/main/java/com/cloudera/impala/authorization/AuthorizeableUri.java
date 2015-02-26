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

/*
 * Class used to authorize access to a URI.
 */
public class AuthorizeableUri implements Authorizeable {
  private final String uriName_;

  public AuthorizeableUri(String uriName) {
    Preconditions.checkNotNull(uriName);
    uriName_ = uriName;
  }

  @Override
  public List<org.apache.sentry.core.Authorizable> getHiveAuthorizeableHierarchy() {
    org.apache.sentry.core.AccessURI accessURI =
        new org.apache.sentry.core.AccessURI(uriName_);
    return Lists.newArrayList((org.apache.sentry.core.Authorizable) accessURI);
  }

  @Override
  public String getName() { return uriName_; }
}