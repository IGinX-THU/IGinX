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
package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.math.BigDecimal;
import java.util.List;

public class TypeConverter {

  public static Object convertToType(DataType type, Object obj) {
    if (obj == null) {
      return null;
    }

    switch (type) {
      case BOOLEAN:
        return toBoolean(obj);
      case INTEGER:
        return toInteger(obj);
      case LONG:
        return toLong(obj);
      case FLOAT:
        return toFloat(obj);
      case DOUBLE:
        return toDouble(obj);
      case BINARY:
        return toBinary(obj);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static Object[] convertToTypes(List<DataType> types, List<Object> objects) {
    if (types.size() != objects.size()) {
      throw new IllegalArgumentException("Types and objects arrays must be of the same length.");
    }

    Object[] results = new Object[objects.size()];
    for (int i = 0; i < objects.size(); i++) {
      results[i] = convertToType(types.get(i), objects.get(i));
    }
    return results;
  }

  private static Boolean toBoolean(Object obj) {
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    } else if (obj instanceof Number) {
      return ((Number) obj).intValue() != 0;
    } else if (obj instanceof String) {
      String s = ((String) obj).toLowerCase();
      return s.equals("true") || s.equals("1");
    } else if (obj instanceof byte[]) {
      String s = (new String((byte[]) obj)).toLowerCase();
      return s.equals("true") || s.equals("1");
    } else {
      throw new IllegalArgumentException("Cannot convert to Boolean: " + obj);
    }
  }

  private static Integer toInteger(Object obj) {
    if (obj instanceof Number) {
      return ((Number) obj).intValue();
    } else if (obj instanceof String) {
      return Integer.parseInt((String) obj);
    } else if (obj instanceof byte[]) {
      return Integer.parseInt(new String((byte[]) obj));
    } else {
      throw new IllegalArgumentException("Cannot convert to Integer: " + obj);
    }
  }

  private static Long toLong(Object obj) {
    if (obj instanceof Number) {
      return ((Number) obj).longValue();
    } else if (obj instanceof String) {
      return Long.parseLong((String) obj);
    } else if (obj instanceof byte[]) {
      return Long.parseLong(new String((byte[]) obj));
    } else {
      throw new IllegalArgumentException("Cannot convert to Long: " + obj);
    }
  }

  private static Float toFloat(Object obj) {
    if (obj instanceof Double) {
      BigDecimal bd = new BigDecimal(obj.toString());
      return bd.floatValue();
    } else if (obj instanceof Number) {
      return ((Number) obj).floatValue();
    } else if (obj instanceof String) {
      return Float.parseFloat((String) obj);
    } else if (obj instanceof byte[]) {
      return Float.parseFloat(new String((byte[]) obj));
    } else {
      throw new IllegalArgumentException("Cannot convert to Float: " + obj);
    }
  }

  private static Double toDouble(Object obj) {
    if (obj instanceof Float) {
      BigDecimal bd = new BigDecimal(obj.toString());
      return bd.doubleValue();
    } else if (obj instanceof Number) {
      return ((Number) obj).doubleValue();
    } else if (obj instanceof String) {
      return Double.parseDouble((String) obj);
    } else {
      throw new IllegalArgumentException("Cannot convert to Double: " + obj);
    }
  }

  private static byte[] toBinary(Object obj) {
    if (obj instanceof byte[]) {
      return (byte[]) obj;
    } else if (obj instanceof String) {
      return ((String) obj).getBytes();
    } else if (obj instanceof Number || obj instanceof Boolean) {
      return obj.toString().getBytes();
    } else {
      throw new IllegalArgumentException("Cannot convert to Binary: " + obj);
    }
  }
}
