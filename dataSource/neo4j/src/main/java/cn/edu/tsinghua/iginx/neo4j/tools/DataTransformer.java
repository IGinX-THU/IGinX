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
package cn.edu.tsinghua.iginx.neo4j.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.nio.charset.StandardCharsets;

public class DataTransformer {

  public static String objectValueToString(Object value) {
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    } else {
      return String.valueOf(value);
    }
  }

  public static Object strValueToDeterminedType(String value, DataType type) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case INTEGER:
        return Integer.parseInt(value);
      case LONG:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BINARY:
      default:
        return value.getBytes(StandardCharsets.UTF_8);
    }
  }

  public static Object objToDeterminedType(Object value, DataType type) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return value;
      case INTEGER:
        return ((Number) value).intValue();
      case LONG:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      case BINARY:
      default:
        return value.toString();
    }
  }

  public static DataType fromStringDataType(String dataType) {
    if (dataType == null) {
      return null;
    }
    switch (dataType.toUpperCase()) {
      case "BOOLEAN":
        return DataType.BOOLEAN;
      case "INTEGER":
        return DataType.INTEGER;
      case "LONG":
        return DataType.LONG;
      case "FLOAT":
        return DataType.FLOAT;
      case "DOUBLE":
        return DataType.DOUBLE;
      case "BINARY":
      default:
        return DataType.BINARY;
    }
  }

  public static String toStringDataType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case BINARY:
      default:
        return "BINARY";
    }
  }

  public static DataType fromObject(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof Float) {
      return DataType.FLOAT;
    }
    if (obj instanceof Double) {
      return DataType.DOUBLE;
    }
    if (obj instanceof Integer) {
      return DataType.INTEGER;
    }
    if (obj instanceof Number) {
      return DataType.LONG;
    }
    if (obj instanceof Boolean) {
      return DataType.BOOLEAN;
    }
    return DataType.BINARY;
  }

  public static Object toIginxType(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }
    return value;
  }

  public static Object fromIginxType(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    return value;
  }
}
