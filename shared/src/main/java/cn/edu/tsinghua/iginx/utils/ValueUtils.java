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

public class ValueUtils {

  public static double transformToDouble(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Null value");
    }

    if (value instanceof byte[]) {
      return Double.parseDouble(new String((byte[]) value));
    } else if (value instanceof Byte) {
      return Double.parseDouble(new String(new byte[] {(byte) value}));
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    } else if (value instanceof Long) {
      return ((Long) value).doubleValue();
    } else if (value instanceof Integer) {
      return ((Integer) value).doubleValue();
    } else if (value instanceof Short) {
      return ((Short) value).doubleValue();
    } else if (value instanceof Double) {
      return (double) value;
    } else if (value instanceof Float) {
      return ((Float) value).doubleValue();
    } else if (value instanceof Boolean) {
      return ((boolean) value) ? 1.0D : 0.0D;
    } else {
      throw new IllegalArgumentException("Unexpected data type");
    }
  }

  public static long transformToLong(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Cannot convert null to long.");
    }

    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Double) {
      return ((Double) value).longValue();
    } else if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof Short) {
      return ((Short) value).longValue();
    } else if (value instanceof Float) {
      return ((Float) value).longValue();
    } else if (value instanceof Boolean) {
      return (Boolean) value ? 1L : 0L;
    } else if (value instanceof Byte) {
      return ((Byte) value).longValue();
    } else if (value instanceof byte[]) {
      return Long.parseLong(new String((byte[]) value));
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    } else {
      throw new IllegalArgumentException(
          "Unsupported type for long conversion: " + value.getClass());
    }
  }

  public static String toString(Object value) {
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    } else {
      return value.toString();
    }
  }
}
