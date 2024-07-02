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
