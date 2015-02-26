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

package com.cloudera.impala.common;

/**
 * "Indirection layer" that allows returning an object via an output
 * parameter of a function call, similar to a pointer or reference parameter
 * in C/C++.
 * Example:
 *   Reference<T> ref = new Reference<T>();
 *   createT(ref);  // calls ref.setRef()
 *   <do something with ref.getRef()>;
 */
public class Reference<RefType> {
<<<<<<< HEAD
  protected RefType ref;

  public Reference(RefType ref) {
    this.ref = ref;
  }

  public Reference() {
    this.ref = null;
  }

  public RefType getRef() {
    return ref;
  }

  public void setRef(RefType ref) {
    this.ref = ref;
  }
=======
  protected RefType ref_;

  public Reference(RefType ref) {
    this.ref_ = ref;
  }

  public Reference() {
    this.ref_ = null;
  }

  public RefType getRef() { return ref_; }
  public void setRef(RefType ref) { this.ref_ = ref; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
