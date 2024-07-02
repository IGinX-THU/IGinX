package cn.edu.tsinghua.iginx.utils;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

import at.favre.lib.bytes.Bytes;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class DataTypeUtils {

  public static boolean isNumber(DataType dataType) {
    return dataType == DataType.INTEGER
        || dataType == DataType.LONG
        || dataType == DataType.FLOAT
        || dataType == DataType.DOUBLE;
  }

  public static boolean isFloatingNumber(DataType dataType) {
    return dataType == DataType.FLOAT || dataType == DataType.DOUBLE;
  }

  public static boolean isWholeNumber(DataType dataType) {
    return dataType == DataType.INTEGER || dataType == DataType.LONG;
  }

  public static DataType getDataTypeFromString(String type) {
    switch (type.toLowerCase()) {
      case "boolean":
        return DataType.BOOLEAN;
      case "integer":
        return DataType.INTEGER;
      case "long":
        return DataType.LONG;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "binary":
        return DataType.BINARY;
      default:
        return null;
    }
  }

  public static DataType getDataTypeFromObject(Object object) {
    if (object instanceof Boolean) {
      return DataType.BOOLEAN;
    } else if (object instanceof Integer) {
      return DataType.INTEGER;
    } else if (object instanceof Long) {
      return DataType.LONG;
    } else if (object instanceof Float) {
      return DataType.FLOAT;
    } else if (object instanceof Double) {
      return DataType.DOUBLE;
    } else if (object instanceof String) {
      return DataType.BINARY;
    } else if (object instanceof byte[]) {
      return DataType.BINARY;
    } else {
      return null;
    }
  }

  public static Object parseStringByDataType(String value, DataType type) {
    switch (type) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case LONG:
        return Long.parseLong(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BINARY:
        // use hex to read binary data from file correctly
        return Bytes.parseHex(value).array();
      case INTEGER:
        return Integer.parseInt(value);
      case FLOAT:
        return Float.parseFloat(value);
      default:
        return value;
    }
  }

  public static String transformObjectToStringByDataType(Object object, DataType type) {
    if (object == null) {
      return null;
    }
    if (type == null) {
      type = BINARY;
    }

    String strValue;
    switch (type) {
      case BINARY:
        // use hex to write binary data into file correctly
        strValue = Bytes.wrap((byte[]) object).encodeHex();
        break;
      case INTEGER:
        strValue = Integer.toString((int) object);
        break;
      case DOUBLE:
        strValue = Double.toString((double) object);
        break;
      case FLOAT:
        strValue = Float.toString((float) object);
        break;
      case BOOLEAN:
        strValue = Boolean.toString((boolean) object);
        break;
      case LONG:
        strValue = Long.toString((long) object);
        break;
      default:
        strValue = null;
        break;
    }

    return strValue;
  }
}
