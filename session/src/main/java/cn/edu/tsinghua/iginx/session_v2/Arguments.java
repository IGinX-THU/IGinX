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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import java.util.List;
import java.util.Objects;

public final class Arguments {

  private Arguments() {}

  public static void checkUrl(final String url, final String name) throws IllegalArgumentException {
    checkNonEmpty(url, name);
    // TODO: 检查 url 合法性
  }

  public static void checkNonEmpty(final String string, final String name)
      throws IllegalArgumentException {
    if (string == null || string.isEmpty()) {
      throw new IllegalArgumentException("Expecting a non-empty string for " + name);
    }
  }

  public static void checkNotNull(final Object obj, final String name) throws NullPointerException {
    Objects.requireNonNull(obj, () -> "Expecting a not null reference for " + name);
  }

  public static void checkDataType(final Object value, final DataType dataType, final String name)
      throws IllegalStateException {
    switch (dataType) {
      case INTEGER:
        if (!(value instanceof Integer)) {
          throw new IllegalArgumentException("Expecting a integer for " + name);
        }
        break;
      case LONG:
        if (!(value instanceof Long)) {
          throw new IllegalArgumentException("Expecting a long for " + name);
        }
        break;
      case BOOLEAN:
        if (!(value instanceof Boolean)) {
          throw new IllegalArgumentException("Expecting a boolean for " + name);
        }
        break;
      case FLOAT:
        if (!(value instanceof Float)) {
          throw new IllegalArgumentException("Expecting a float for " + name);
        }
        break;
      case DOUBLE:
        if (!(value instanceof Double)) {
          throw new IllegalArgumentException("Expecting a double for " + name);
        }
        break;
      case BINARY:
        if (!(value instanceof byte[])) {
          throw new IllegalArgumentException("Expecting a byte array for " + name);
        }
        break;
    }
  }

  public static <T> void checkListNonEmpty(final List<T> list, final String name) {
    if (list == null || list.isEmpty()) {
      throw new IllegalArgumentException("Expecting a non-empty list for " + name);
    }
  }

  public static void checkTaskType(TaskType expected, TaskType actual) {
    if (actual != null && !expected.equals(actual)) {
      throw new IllegalArgumentException(
          "Expecting task type: " + expected + ", actual: " + actual);
    }
  }
}
