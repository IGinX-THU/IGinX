/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Value {

  private final DataType dataType;

  private Boolean boolV;

  private Integer intV;

  private Long longV;

  private Float floatV;

  private Double doubleV;

  private byte[] binaryV;

  private Object objectV;

  public Value(DataType dataType, Object value) {
    this.dataType = dataType;
    switch (dataType) {
      case INTEGER:
        intV = (Integer) value;
        break;
      case LONG:
        longV = (Long) value;
        break;
      case DOUBLE:
        doubleV = (Double) value;
        break;
      case BINARY:
        binaryV = (byte[]) value;
        break;
      case BOOLEAN:
        boolV = (Boolean) value;
        break;
      case FLOAT:
        floatV = (Float) value;
        break;
      default:
        throw new IllegalArgumentException("unknown data type: " + dataType);
    }
  }

  public Value(Object v) {
    if (v instanceof Boolean) {
      this.dataType = DataType.BOOLEAN;
      this.boolV = (Boolean) v;
    } else if (v instanceof Integer) {
      this.dataType = DataType.INTEGER;
      this.intV = (Integer) v;
    } else if (v instanceof Long) {
      this.dataType = DataType.LONG;
      this.longV = (Long) v;
    } else if (v instanceof Float) {
      this.dataType = DataType.FLOAT;
      this.floatV = (Float) v;
    } else if (v instanceof Double) {
      this.dataType = DataType.DOUBLE;
      this.doubleV = (Double) v;
    } else if (v instanceof byte[]) {
      this.dataType = DataType.BINARY;
      this.binaryV = (byte[]) v;
    } else {
      this.dataType = null;
      this.objectV = v;
    }
  }

  public Value(boolean boolV) {
    this.dataType = DataType.BOOLEAN;
    this.boolV = boolV;
  }

  public Value(int intV) {
    this.dataType = DataType.INTEGER;
    this.intV = intV;
  }

  public Value(long longV) {
    this.dataType = DataType.LONG;
    this.longV = longV;
  }

  public Value(float floatV) {
    this.dataType = DataType.FLOAT;
    this.floatV = floatV;
  }

  public Value(double doubleV) {
    this.dataType = DataType.DOUBLE;
    this.doubleV = doubleV;
  }

  public Value(String binaryV) {
    this.dataType = DataType.BINARY;
    this.binaryV = binaryV.getBytes(StandardCharsets.UTF_8);
  }

  public Value(byte[] binaryV) {
    this.dataType = DataType.BINARY;
    this.binaryV = binaryV;
  }

  public Object getValue() {
    if (dataType == null) {
      return objectV;
    }
    switch (dataType) {
      case BINARY:
        return binaryV;
      case BOOLEAN:
        return boolV;
      case INTEGER:
        return intV;
      case LONG:
        return longV;
      case DOUBLE:
        return doubleV;
      case FLOAT:
        return floatV;
      default:
        return null;
    }
  }

  public DataType getDataType() {
    return dataType;
  }

  public Boolean getBoolV() {
    return boolV;
  }

  public Integer getIntV() {
    return intV;
  }

  public Long getLongV() {
    return longV;
  }

  public Float getFloatV() {
    return floatV;
  }

  public Double getDoubleV() {
    return doubleV;
  }

  public byte[] getBinaryV() {
    return binaryV;
  }

  public String getBinaryVAsString() {
    return new String(binaryV);
  }

  public boolean isNull() {
    switch (dataType) {
      case INTEGER:
        return intV == null;
      case LONG:
        return longV == null;
      case BOOLEAN:
        return boolV == null;
      case FLOAT:
        return floatV == null;
      case DOUBLE:
        return doubleV == null;
      case BINARY:
        return binaryV == null;
    }
    return true;
  }

  public String getAsString() {
    if (isNull()) {
      return "";
    }
    switch (dataType) {
      case BINARY:
        return new String(binaryV);
      case LONG:
        return longV.toString();
      case INTEGER:
        return intV.toString();
      case DOUBLE:
        return doubleV.toString();
      case FLOAT:
        return floatV.toString();
      case BOOLEAN:
        return boolV.toString();
      default:
        return "";
    }
  }

  public String toString() {
    return getAsString();
  }

  public Value copy() {
    return new Value(this.getValue());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Value value = (Value) o;
    return dataType == value.dataType
        && (boolV == null || boolV.equals(value.boolV))
        && (intV == null || intV.equals(value.intV))
        && (longV == null || longV.equals(value.longV))
        && (floatV == null || floatV.equals(value.floatV))
        && (doubleV == null || doubleV.equals(value.doubleV))
        && (binaryV == null || Arrays.equals(binaryV, value.binaryV));
  }
}
