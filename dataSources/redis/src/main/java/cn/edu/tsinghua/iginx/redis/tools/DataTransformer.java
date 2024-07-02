package cn.edu.tsinghua.iginx.redis.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;

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
        return value.getBytes();
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
}
