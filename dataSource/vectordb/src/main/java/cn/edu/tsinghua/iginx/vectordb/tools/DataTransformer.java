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
package cn.edu.tsinghua.iginx.vectordb.tools;

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
    switch (dataType) {
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

  public static DataType fromMilvusDataType(String dataType) {
    if (dataType == null) {
      return null;
    }
    switch (dataType) {
      case "Bool":
        return DataType.BOOLEAN;
      case "Int8":
      case "Int16":
      case "Int32":
        return DataType.INTEGER;
      case "Int64":
        return DataType.LONG;
      case "Float":
        return DataType.FLOAT;
      case "Double":
        return DataType.DOUBLE;
      case "BinaryVector":
      case "FloatVector":
      case "Float16Vector":
      case "BFloat16Vector":
      case "SparseFloatVector":
      case "String":
      case "VarChar":
      case "Array":
      case "JSON":
      case "None":
      default:
        return DataType.BINARY;
    }
  }

  public static DataType fromMilvusDataType(io.milvus.v2.common.DataType dataType) {
    if (dataType == null) {
      return null;
    }
    switch (dataType.name()) {
      case "Bool":
        return DataType.BOOLEAN;
      case "Int8":
      case "Int16":
      case "Int32":
        return DataType.INTEGER;
      case "Int64":
        return DataType.LONG;
      case "Float":
        return DataType.FLOAT;
      case "Double":
        return DataType.DOUBLE;
      case "BinaryVector":
      case "FloatVector":
      case "Float16Vector":
      case "BFloat16Vector":
      case "SparseFloatVector":
      case "String":
      case "VarChar":
      case "Array":
      case "JSON":
      case "None":
      default:
        return DataType.BINARY;
    }
  }

  public static String toMilvusDataTypeString(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "Bool";
      case INTEGER:
        return "Int32";
      case LONG:
        return "Int64";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
      case BINARY:
      default:
        return "VarChar";
    }
  }

  public static io.milvus.v2.common.DataType toMilvusDataType(DataType dataType) {
    if (dataType == null) {
      return io.milvus.v2.common.DataType.VarChar;
    }
    switch (dataType) {
      case BOOLEAN:
        return io.milvus.v2.common.DataType.Bool;
      case INTEGER:
        return io.milvus.v2.common.DataType.Int32;
      case LONG:
        return io.milvus.v2.common.DataType.Int64;
      case FLOAT:
        return io.milvus.v2.common.DataType.Float;
      case DOUBLE:
        return io.milvus.v2.common.DataType.Double;
      case BINARY:
      default:
        return io.milvus.v2.common.DataType.VarChar;
    }
  }

  public static String milvusDataTypeToIginxDataType(String dataType) {
    return toStringDataType(fromMilvusDataType(dataType));
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
}
