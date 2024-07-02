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
}
