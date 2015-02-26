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

import com.cloudera.impala.catalog.Column;
<<<<<<< HEAD
import com.cloudera.impala.catalog.PrimitiveType;
=======
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Type;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.cloudera.impala.thrift.TSlotDescriptor;
import com.google.common.base.Objects;

public class SlotDescriptor {
<<<<<<< HEAD
  private final SlotId id;
  private final TupleDescriptor parent;
  private PrimitiveType type;
  private Column column;  // underlying column, if there is one

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized;

  // if false, this slot cannot be NULL
  private boolean isNullable;

  // physical layout parameters
  private int byteSize;
  private int byteOffset;  // within tuple
  private int nullIndicatorByte;  // index into byte array
  private int nullIndicatorBit; // index within byte
  private int slotIdx;          // index within tuple struct

  SlotDescriptor(int id, TupleDescriptor parent) {
    this.id = new SlotId(id);
    this.parent = parent;
    this.byteOffset = -1;  // invalid
    this.isMaterialized = false;
    this.isNullable = true;
  }

  public int getNullIndicatorByte() {
    return nullIndicatorByte;
  }

  public void setNullIndicatorByte(int nullIndicatorByte) {
    this.nullIndicatorByte = nullIndicatorByte;
  }

  public int getNullIndicatorBit() {
    return nullIndicatorBit;
  }

  public void setNullIndicatorBit(int nullIndicatorBit) {
    this.nullIndicatorBit = nullIndicatorBit;
  }

  public SlotId getId() {
    return id;
  }

  public TupleDescriptor getParent() {
    return parent;
  }

  public PrimitiveType getType() {
    return type;
  }

  public void setType(PrimitiveType type) {
    this.type = type;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
    this.type = column.getType();
  }

  public boolean getIsMaterialized() {
    return isMaterialized;
  }

  public void setIsMaterialized(boolean value) {
    isMaterialized = value;
  }

  public boolean getIsNullable() {
    return isNullable;
  }

  public void setIsNullable(boolean value) {
    isNullable = value;
  }

  public int getByteSize() {
    return byteSize;
  }

  public void setByteSize(int byteSize) {
    this.byteSize = byteSize;
  }

  public int getByteOffset() {
    return byteOffset;
  }

  public void setByteOffset(int byteOffset) {
    this.byteOffset = byteOffset;
  }

  public void setSlotIdx(int slotIdx) {
    this.slotIdx = slotIdx;
=======
  private final SlotId id_;
  private final TupleDescriptor parent_;
  private Type type_;
  private Column column_;  // underlying column, if there is one
  private String label_;  // for SlotRef.toSql() in absence of column name

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized_ = false;

  // if false, this slot cannot be NULL
  private boolean isNullable_ = true;

  // physical layout parameters
  private int byteSize_;
  private int byteOffset_;  // within tuple
  private int nullIndicatorByte_;  // index into byte array
  private int nullIndicatorBit_; // index within byte
  private int slotIdx_;          // index within tuple struct

  private ColumnStats stats_;  // only set if 'column' isn't set

  SlotDescriptor(SlotId id, TupleDescriptor parent) {
    id_ = id;
    parent_ = parent;
    byteOffset_ = -1;  // invalid
  }

  SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
    id_ = id;
    parent_ = parent;
    type_ = src.type_;
    column_ = src.column_;
    label_ = src.label_;
    isMaterialized_ = src.isMaterialized_;
    isNullable_ = src.isNullable_;
    byteSize_ = src.byteSize_;
    byteOffset_ = src.byteOffset_;
    nullIndicatorByte_ = src.nullIndicatorByte_;
    nullIndicatorBit_ = src.nullIndicatorBit_;
    slotIdx_ = src.slotIdx_;
    stats_ = src.stats_;
  }

  public int getNullIndicatorByte() { return nullIndicatorByte_; }
  public void setNullIndicatorByte(int nullIndicatorByte) {
    this.nullIndicatorByte_ = nullIndicatorByte;
  }
  public int getNullIndicatorBit() { return nullIndicatorBit_; }
  public void setNullIndicatorBit(int nullIndicatorBit) {
    this.nullIndicatorBit_ = nullIndicatorBit;
  }
  public SlotId getId() { return id_; }
  public TupleDescriptor getParent() { return parent_; }
  public Type getType() { return type_; }
  public void setType(Type type) { this.type_ = type; }
  public Column getColumn() { return column_; }
  public void setColumn(Column column) {
    this.column_ = column;
    this.type_ = column.getType();
  }
  public boolean isMaterialized() { return isMaterialized_; }
  public void setIsMaterialized(boolean value) { isMaterialized_ = value; }
  public boolean getIsNullable() { return isNullable_; }
  public void setIsNullable(boolean value) { isNullable_ = value; }
  public int getByteSize() { return byteSize_; }
  public void setByteSize(int byteSize) { this.byteSize_ = byteSize; }
  public int getByteOffset() { return byteOffset_; }
  public void setByteOffset(int byteOffset) { this.byteOffset_ = byteOffset; }
  public void setSlotIdx(int slotIdx) { this.slotIdx_ = slotIdx; }
  public String getLabel() { return label_; }
  public void setLabel(String label) { this.label_ = label; }
  public void setStats(ColumnStats stats) { this.stats_ = stats; }

  public ColumnStats getStats() {
    if (stats_ == null) {
      if (column_ != null) {
        stats_ = column_.getStats();
      } else {
        stats_ = new ColumnStats(type_);
      }
    }
    return stats_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  public TSlotDescriptor toThrift() {
    return new TSlotDescriptor(
<<<<<<< HEAD
        id.asInt(), parent.getId().asInt(), type.toThrift(),
        ((column != null) ? column.getPosition() : -1),
        byteOffset, nullIndicatorByte, nullIndicatorBit,
        slotIdx, isMaterialized);
  }

  public String debugString() {
    String colStr = (column == null ? "null" : column.getName());
    String typeStr = (type == null ? "null" : type.toString());
    return Objects.toStringHelper(this)
        .add("id", id.asInt())
        .add("col", colStr)
        .add("type", typeStr)
        .add("materialized", isMaterialized)
        .add("byteSize", byteSize)
        .add("byteOffset", byteOffset)
        .add("nullIndicatorByte", nullIndicatorByte)
        .add("nullIndicatorBit", nullIndicatorBit)
        .add("slotIdx", slotIdx)
=======
        id_.asInt(), parent_.getId().asInt(), type_.toThrift(),
        ((column_ != null) ? column_.getPosition() : -1),
        byteOffset_, nullIndicatorByte_, nullIndicatorBit_,
        slotIdx_, isMaterialized_);
  }

  public String debugString() {
    String colStr = (column_ == null ? "null" : column_.getName());
    String typeStr = (type_ == null ? "null" : type_.toString());
    return Objects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("col", colStr)
        .add("type", typeStr)
        .add("materialized", isMaterialized_)
        .add("byteSize", byteSize_)
        .add("byteOffset", byteOffset_)
        .add("nullIndicatorByte", nullIndicatorByte_)
        .add("nullIndicatorBit", nullIndicatorBit_)
        .add("slotIdx", slotIdx_)
        .add("stats", stats_)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        .toString();
  }
}
