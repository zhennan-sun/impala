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
package org.apache.sentry.core;

public class AccessURI implements Authorizable {
  /**
   * Represents all URIs
   */
  public static final AccessURI ALL = new AccessURI(AccessConstants.ALL);

  private final String uriName;


  public AccessURI(String uriName) {
    uriName = uriName == null ? "" : uriName;
    if(!(uriName.equals(AccessConstants.ALL) || 
        uriName.startsWith("file://") ||
        uriName.startsWith("hdfs://"))) {
      throw new IllegalArgumentException("URI '" + uriName + "' in invalid. Must start with file:// or hdfs://");      
    }
    this.uriName = uriName;
  }

  @Override
  public String getName() {
    return uriName;
  }

  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.URI;
  }

  @Override
  public String toString() {
    return "URI [name=" + uriName + "]";
  }

}
