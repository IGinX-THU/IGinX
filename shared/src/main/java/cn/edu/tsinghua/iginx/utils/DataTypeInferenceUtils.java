package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class DataTypeInferenceUtils {

  private static boolean isBoolean(String s) {
    return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
  }

  private static boolean isNumber(String s) {
    if (s == null || s.equalsIgnoreCase("nan")) {
      return false;
    }
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isLong(String s) {
    return Long.parseLong(s) > (2 << 24);
  }

  public static DataType getInferredDataType(String s) {
    if (isBoolean(s)) {
      return DataType.BOOLEAN;
    } else if (isNumber(s)) {
      if (!s.contains(".") && !s.equals("0")) {
        if (isLong(s)) {
          return DataType.LONG;
        }
        return DataType.INTEGER;
      } else {
        return DataType.DOUBLE;
      }
    } else if (s.equalsIgnoreCase("null")) {
      return null;
    } else {
      return DataType.BINARY;
    }
  }
}
