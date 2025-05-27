/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import java.util.ArrayList;
import java.util.List;
import shaded.iginx.org.apache.parquet.io.api.*;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

class IginxRecordMaterializer extends RecordMaterializer<IginxGroup> {

  private final IginxMessageConverter root;

  public IginxRecordMaterializer(MessageType schema) {
    this.root = new IginxMessageConverter(schema);
  }

  @Override
  public void skipCurrentRecord() {}

  @Override
  public IginxGroup getCurrentRecord() {
    return root.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

  static class IginxMessageConverter extends IginxGroupConverter {
    protected IginxGroup currentRecord;

    public IginxMessageConverter(GroupType groupType) {
      super(groupType, null);
    }

    public IginxGroup getCurrentRecord() {
      return currentRecord;
    }

    @Override
    public void start() {
      super.start();
      currentRecord = null;
    }

    @Override
    public void end() {
      currentRecord = buildGroup();
    }
  }

  static class IginxGroupConverter extends GroupConverter {
    protected final GroupType groupType;
    protected final Converter[] converters;
    protected final ValueHolder[] subValueHolders;
    protected final ValueHolder valueHolder;

    public IginxGroupConverter(GroupType groupType, ValueHolder valueHolder) {
      this.groupType = groupType;
      this.valueHolder = valueHolder;
      this.converters = new Converter[groupType.getFieldCount()];
      this.subValueHolders = new ValueHolder[groupType.getFieldCount()];
      for (int fieldIndex = 0; fieldIndex < groupType.getFieldCount(); fieldIndex++) {
        ValueHolder subValueHolder = new ValueHolder();
        subValueHolders[fieldIndex] = subValueHolder;
        Type fieldType = groupType.getType(fieldIndex);
        if (fieldType.isPrimitive()) {
          converters[fieldIndex] =
              new IginxPrimitiveConverter(fieldType.asPrimitiveType(), subValueHolder);
        } else {
          converters[fieldIndex] = new IginxGroupConverter(fieldType.asGroupType(), subValueHolder);
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      for (ValueHolder subValueHolder : subValueHolders) {
        subValueHolder.reset();
      }
    }

    @Override
    public void end() {
      valueHolder.addValue(buildGroup());
    }

    protected IginxGroup buildGroup() {
      Object[] values = new Object[groupType.getFieldCount()];
      for (int fieldIndex = 0; fieldIndex < groupType.getFieldCount(); fieldIndex++) {
        ValueHolder subValueHolder = subValueHolders[fieldIndex];
        Type fieldType = groupType.getType(fieldIndex);
        switch (fieldType.getRepetition()) {
          case REQUIRED:
          case OPTIONAL:
            values[fieldIndex] = subValueHolder.getValue();
            break;
          case REPEATED:
            values[fieldIndex] = subValueHolder.getValues();
            break;
          default:
            throw new IllegalArgumentException("Unsupported repetition of type: " + fieldType);
        }
      }
      return new IginxGroup(groupType, values);
    }
  }

  public static class IginxPrimitiveConverter extends PrimitiveConverter {

    private final PrimitiveType primitiveType;
    private final ValueHolder valueHolder;

    public IginxPrimitiveConverter(PrimitiveType primitiveType, ValueHolder valueHolder) {
      this.primitiveType = primitiveType;
      this.valueHolder = valueHolder;
    }

    public void addBinary(Binary value) {
      valueHolder.addValue(value.getBytes());
    }

    public void addBoolean(boolean value) {
      valueHolder.addValue(value);
    }

    public void addDouble(double value) {
      valueHolder.addValue(value);
    }

    public void addFloat(float value) {
      valueHolder.addValue(value);
    }

    public void addInt(int value) {
      valueHolder.addValue(value);
    }

    public void addLong(long value) {
      valueHolder.addValue(value);
    }
  }

  static class ValueHolder {
    private List<Object> values = new ArrayList<>();

    public void addValue(Object value) {
      values.add(value);
    }

    public void reset() {
      values = new ArrayList<>();
    }

    public List<Object> getValues() {
      return values;
    }

    public Object getValue() {
      if (values.size() == 1) {
        return values.get(0);
      } else if (values.isEmpty()) {
        return null;
      } else {
        throw new IllegalStateException("ValueHolder contains multiple values: " + values);
      }
    }
  }
}
