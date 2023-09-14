/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.nio.charset.StandardCharsets;

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

  public byte[] castToByteArray() {
    byte[] bytes = new byte[0];
    if (isNull()) {
      return bytes;
    }
    switch (dataType) {
      case INTEGER:
        bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
          bytes[i] = (byte) ((intV >>> 8 * i) & 0xff);
        }
        return bytes;
      case LONG:
        bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
          bytes[i] = (byte) ((longV >>> 8 * i) & 0xff);
        }
        return bytes;
      case BOOLEAN:
        bytes = new byte[1];
        bytes[0] = (byte) (boolV ? 0x01 : 0x00);
        return bytes;
      case FLOAT:
        int valueInt = Float.floatToIntBits(floatV);
        bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
          bytes[i] = (byte) ((valueInt >>> 8 * i) & 0xff);
        }
        return bytes;
      case DOUBLE:
        long valueLong = Double.doubleToRawLongBits(doubleV);
        bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
          bytes[i] = (byte) ((valueLong >>> 8 * i) & 0xff);
        }
        return bytes;
      case BINARY:
        return binaryV;
      default:
        return bytes;
    }
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

  public Value copy() {
    return new Value(this.getValue());
  }
}
