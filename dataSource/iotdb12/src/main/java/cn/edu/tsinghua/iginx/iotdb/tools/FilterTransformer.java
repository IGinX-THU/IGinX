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
package cn.edu.tsinghua.iginx.iotdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class FilterTransformer {

  public static String toString(Filter filter) {
    if (filter == null) {
      return "";
    }
    switch (filter.getType()) {
      case And:
        return toString((AndFilter) filter);
      case Or:
        return toString((OrFilter) filter);
      case Not:
        return toString((NotFilter) filter);
      case Value:
        return toString((ValueFilter) filter);
      case Key:
        return toString((KeyFilter) filter);
      case In:
        return toString((InFilter) filter);
      default:
        return "";
    }
  }

  private static String toString(AndFilter filter) {
    StringBuilder sb = new StringBuilder();
    for (Filter child : filter.getChildren()) {
      String filterStr = toString(child);
      if (!filterStr.isEmpty()) {
        sb.append(toString(child)).append(" and ");
      }
    }

    if (sb.length() == 0) {
      return "";
    }

    return "(" + sb.substring(0, sb.length() - 4) + ")";
  }

  private static String toString(NotFilter filter) {
    String childStr = toString(filter.getChild());

    if (childStr.isEmpty()) {
      return "";
    }

    return "not " + childStr;
  }

  private static String toString(KeyFilter filter) {
    return "time " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + filter.getValue();
  }

  private static String toString(ValueFilter filter) {
    String value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "'" + filter.getValue().getBinaryVAsString() + "'"
            : filter.getValue().getValue().toString();

    switch (filter.getOp()) {
      case LIKE:
      case LIKE_AND:
        if (!value.endsWith("$'")) {
          value = value.substring(0, value.length() - 1) + "$'";
        }
        return filter.getPath() + " regexp " + value;
      case NOT_LIKE:
      case NOT_LIKE_AND:
        // iotdb12不支持使用not like
        return "";
      default:
        return filter.getPath() + " " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + value;
    }
  }

  private static String toString(OrFilter filter) {
    StringBuilder sb = new StringBuilder();
    for (Filter child : filter.getChildren()) {
      String filterStr = toString(child);
      if (!filterStr.isEmpty()) {
        sb.append(toString(child)).append(" or ");
      }
    }

    if (sb.length() == 0) {
      return "";
    }

    return "(" + sb.substring(0, sb.length() - 4) + ")";
  }

  private static String toString(InFilter filter) {
    StringBuilder sb = new StringBuilder();
    sb.append(filter.getPath());

    if (filter.getInOp().isNotOp()) {
      sb.append(" not in (");
    } else {
      sb.append(" in (");
    }

    for (Value value : filter.getValues()) {
      if (value.getDataType() == DataType.BINARY) {
        sb.append("'").append(value.getBinaryVAsString()).append("', ");
      } else {
        sb.append(value).append(", ");
      }
    }

    return sb.substring(0, sb.length() - 2) + ")";
  }
}
