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
package cn.edu.tsinghua.iginx.session_v2.write;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.HashMap;
import java.util.Map;

public class Point {

  private final long key;

  private final Object value;

  private final DataType dataType;

  private final String measurement;

  private final Map<String, String> tags;

  private final String fullName;

  public Point(
      long key, Object value, DataType dataType, String measurement, Map<String, String> tags) {
    this.key = key;
    this.value = value;
    this.dataType = dataType;
    this.measurement = measurement;
    this.tags = tags;
    this.fullName = TagKVUtils.toFullName(measurement, tags);
  }

  private Point(Point.Builder builder) {
    this(builder.key, builder.value, builder.dataType, builder.measurement, builder.tags);
  }

  public static Point.Builder builder() {
    return new Point.Builder();
  }

  public long getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  public DataType getDataType() {
    return dataType;
  }

  public String getMeasurement() {
    return measurement;
  }

  public String getFullName() {
    return fullName;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public static class Builder {

    private long key = -1;

    private Object value;

    private DataType dataType;

    private String measurement;

    private final Map<String, String> tags = new HashMap<>();

    private Builder() {}

    public Point.Builder key(long key) {
      this.key = key;
      return this;
    }

    public Point.Builder now() {
      this.key = System.currentTimeMillis();
      return this;
    }

    public Point.Builder dataType(DataType dataType) {
      Arguments.checkNotNull(dataType, "dataType");
      this.dataType = dataType;
      return this;
    }

    public Point.Builder value(Object value) {
      Arguments.checkNotNull(value, "value");
      this.value = value;
      return this;
    }

    public Point.Builder measurement(String measurement) {
      Arguments.checkNonEmpty(measurement, "measurement");
      this.measurement = measurement;
      return this;
    }

    public Point.Builder booleanValue(boolean value) {
      this.value = value;
      this.dataType = DataType.BOOLEAN;
      return this;
    }

    public Point.Builder intValue(int value) {
      this.value = value;
      this.dataType = DataType.INTEGER;
      return this;
    }

    public Point.Builder longValue(long value) {
      this.value = value;
      this.dataType = DataType.LONG;
      return this;
    }

    public Point.Builder floatValue(float value) {
      this.value = value;
      this.dataType = DataType.FLOAT;
      return this;
    }

    public Point.Builder doubleValue(double value) {
      this.value = value;
      this.dataType = DataType.DOUBLE;
      return this;
    }

    public Point.Builder binaryValue(byte[] value) {
      this.value = value;
      this.dataType = DataType.BINARY;
      return this;
    }

    public Point.Builder tagKV(String tagK, String tagV) {
      this.tags.put(tagK, tagV);
      return this;
    }

    public Point build() {
      Arguments.checkNonEmpty(measurement, "measurement");
      Arguments.checkNotNull(value, "value");
      Arguments.checkNotNull(dataType, "dataType");
      Arguments.checkDataType(value, dataType, "value");
      if (key < 0) {
        key = System.currentTimeMillis();
      }
      return new Point(this);
    }
  }
}
