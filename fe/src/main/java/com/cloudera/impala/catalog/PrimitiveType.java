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

package com.cloudera.impala.catalog;

<<<<<<< HEAD
import java.util.ArrayList;

import com.cloudera.impala.analysis.SqlParserSymbols;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.base.Preconditions;
=======
import java.util.List;

import com.cloudera.impala.thrift.TPrimitiveType;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.collect.Lists;

public enum PrimitiveType {
  INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE),
<<<<<<< HEAD
=======
  // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
  // with all other types.
  NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE),
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN),
  TINYINT("TINYINT", 1, TPrimitiveType.TINYINT),
  SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT),
  INT("INT", 4, TPrimitiveType.INT),
  BIGINT("BIGINT", 8, TPrimitiveType.BIGINT),
  FLOAT("FLOAT", 4, TPrimitiveType.FLOAT),
  DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE),
  DATE("DATE", 4, TPrimitiveType.DATE),
  DATETIME("DATETIME", 8, TPrimitiveType.DATETIME),
  // The timestamp structure is 12 bytes, Aligning to 8 bytes makes it 16.
  TIMESTAMP("TIMESTAMP", 16, TPrimitiveType.TIMESTAMP),
  // 8-byte pointer and 4-byte length indicator (12 bytes total).
  // Aligning to 8 bytes so 16 total.
<<<<<<< HEAD
  STRING("STRING", 16, TPrimitiveType.STRING);

  private final String description;
  private final int slotSize;  // size of tuple slot for this type
  private final TPrimitiveType thriftType;

  private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
    this.description = description;
    this.slotSize = slotSize;
    this.thriftType = thriftType;
  }

  @Override
  public String toString() {
    return description;
  }

  public TPrimitiveType toThrift() {
    return thriftType;
  }

  public int getSlotSize() {
    return slotSize;
  }

  public static int getMaxSlotSize() {
    return STRING.slotSize;
  }

  public boolean isFixedPointType() {
    return this == TINYINT || this == SMALLINT || this == INT || this == BIGINT;
  }

  public boolean isFloatingPointType() {
    return this == FLOAT || this == DOUBLE;
  }

  public PrimitiveType getMaxResolutionType() {
    if (isFixedPointType()) {
      return BIGINT;
    // Timestamps get summed as DOUBLE for AVG.
    } else if (isFloatingPointType() || this == TIMESTAMP) {
      return DOUBLE;
    } else {
      return INVALID_TYPE;
    }
  }

  public boolean isNumericType() {
    return isFixedPointType() || isFloatingPointType();
  }

  public boolean isValid() {
    return this != INVALID_TYPE;
  }

  public boolean isDateType() {
    return (this == DATE || this == DATETIME || this == TIMESTAMP);
  }

  public boolean isStringType() {
    return (this == STRING);
  }

  private static ArrayList<PrimitiveType> numericTypes;
  static {
    numericTypes = Lists.newArrayList();
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);
  }

  public static ArrayList<PrimitiveType> getNumericTypes() {
    return numericTypes;
  }

  private static ArrayList<PrimitiveType> fixedPointTypes;
  static {
    fixedPointTypes = Lists.newArrayList();
    fixedPointTypes.add(TINYINT);
    fixedPointTypes.add(SMALLINT);
    fixedPointTypes.add(INT);
    fixedPointTypes.add(BIGINT);
  }

  public static ArrayList<PrimitiveType> getFixedPointTypes() {
    return fixedPointTypes;
  }

  /**
   * Matrix that records "smallest" assignment-compatible type of two types
   * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
   * incompatible). A value of any of the two types could be assigned to a slot
   * of the assignment-compatible type without loss of precision.
   *
   * We chose not to follow MySQL's type casting behavior as described here:
   * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
   * for the following reasons:
   * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
   * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
   * special cases when dealing with dates and timestamps
   */
  private static PrimitiveType[][] compatibilityMatrix;
  static {
    compatibilityMatrix = new PrimitiveType[STRING.ordinal() + 1][STRING.ordinal() + 1];

    compatibilityMatrix[BOOLEAN.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
    compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = TINYINT;
    compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[TINYINT.ordinal()][TINYINT.ordinal()] = TINYINT;
    compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[SMALLINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[INT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = DATE;
    compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = DATETIME;
    compatibilityMatrix[DATE.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = DATETIME;
    compatibilityMatrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[TIMESTAMP.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] = TIMESTAMP;

    compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = STRING;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   */
  public static PrimitiveType getAssignmentCompatibleType(PrimitiveType t1, PrimitiveType t2) {
    if (!t1.isValid() || !t2.isValid()) {
      return INVALID_TYPE;
    }

    PrimitiveType smallerType = (t1.ordinal() < t2.ordinal() ? t1 : t2);
    PrimitiveType largerType = (t1.ordinal() > t2.ordinal() ? t1 : t2);
    PrimitiveType result =  compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    Preconditions.checkNotNull(result);
    return result;
  }

  /**
   * Returns if it is compatible to implicitly cast from t1 to t2 (casting from
   * t1 to t2 results in no loss of precision.
   */
  public static boolean isImplicitlyCastable(PrimitiveType t1, PrimitiveType t2) {
      return getAssignmentCompatibleType(t1, t2) == t2;
  }

  // Returns the highest resolution type
  // corresponding to the lexer symbol of numeric literals.
  // Currently used to determine whether the literal is fixed or floating point.
  public static PrimitiveType literalSymbolIdToType(int symbolId) {
    switch (symbolId) {
      case SqlParserSymbols.INTEGER_LITERAL:
        return BIGINT;
      case SqlParserSymbols.FLOATINGPOINT_LITERAL:
        return DOUBLE;
      default:
        return INVALID_TYPE;
    }
  }
}

=======
  STRING("STRING", 16, TPrimitiveType.STRING),
  VARCHAR("VARCHAR", 16, TPrimitiveType.VARCHAR),

  // Unsupported scalar type.
  BINARY("BINARY", -1, TPrimitiveType.BINARY),

  // For decimal at the highest precision, the BE uses 16 bytes.
  DECIMAL("DECIMAL", 16, TPrimitiveType.DECIMAL),

  // Fixed length char array.
  CHAR("CHAR", -1, TPrimitiveType.CHAR);

  private final String description_;
  private final int slotSize_;  // size of tuple slot for this type
  private final TPrimitiveType thriftType_;

  private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
    description_ = description;
    slotSize_ = slotSize;
    thriftType_ = thriftType;
  }

  @Override
  public String toString() {
    return description_;
  }

  public static PrimitiveType fromThrift(TPrimitiveType t) {
    switch (t) {
      case INVALID_TYPE: return INVALID_TYPE;
      case NULL_TYPE: return NULL_TYPE;
      case BOOLEAN: return BOOLEAN;
      case TINYINT: return TINYINT;
      case SMALLINT: return SMALLINT;
      case INT: return INT;
      case BIGINT: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case STRING: return STRING;
      case VARCHAR: return VARCHAR;
      case TIMESTAMP: return TIMESTAMP;
      case CHAR: return CHAR;
      case DECIMAL: return DECIMAL;
      case BINARY: return BINARY;
    }
    return INVALID_TYPE;
  }

  public TPrimitiveType toThrift() { return thriftType_; }

  public static List<TPrimitiveType> toThrift(PrimitiveType[] types) {
    List<TPrimitiveType> result = Lists.newArrayList();
    for (PrimitiveType t: types) {
      result.add(t.toThrift());
    }
    return result;
  }

  public int getSlotSize() { return slotSize_; }
  public static int getMaxSlotSize() { return DECIMAL.slotSize_; }
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
