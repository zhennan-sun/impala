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

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
<<<<<<< HEAD
import com.cloudera.impala.common.InternalException;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

public interface ParseNode {

  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @param analyzer
<<<<<<< HEAD
   * @throws AnalysisException, InternalException
   */
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException;
=======
   * @throws AnalysisException
   */
  public void analyze(Analyzer analyzer) throws AnalysisException;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  /**
   * @return SQL syntax corresponding to this node.
   */
  public String toSql();
<<<<<<< HEAD

  public String debugString();
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
