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
package cn.edu.tsinghua.iginx.influxdb.tools;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.isLikeOp;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
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
      case Path:
        return toString((PathFilter) filter);
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
    return "not " + filter.toString();
  }

  private static String toString(KeyFilter filter) {
    return "r[\"_time\"] "
        + Op.op2StrWithoutAndOr(filter.getOp())
        + " time(v: "
        + filter.getValue()
        + ")";
  }

  private static String toString(ValueFilter filter) {
    // path 获取的是 table.field，需要删掉.前面的table名。
    InfluxDBSchema schema = new InfluxDBSchema(filter.getPath());
    String path = schema.getFieldString();
    String value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "\"" + filter.getValue().getBinaryVAsString() + "\""
            : filter.getValue().getValue().toString();

    if (isLikeOp(filter.getOp())) {
      return "r[\""
          + path
          + "\"] "
          + " =~ /"
          + filter.getValue().getBinaryVAsString()
          + "$/"; // SQL的正则匹配需要全部匹配，但InfluxDB可以部分匹配，所以需要在最后加上$以保证匹配全部字符串。
    }

    return "r[\"" + path + "\"] " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + value;
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

  private static String toString(PathFilter filter) {
    // path 获取的是 table.field，需要删掉.前面的table名。
    InfluxDBSchema schemaA = new InfluxDBSchema(filter.getPathA());
    InfluxDBSchema schemaB = new InfluxDBSchema(filter.getPathB());
    String pathA = schemaA.getFieldString();
    String pathB = schemaB.getFieldString();

    return "r[\""
        + pathA
        + "\"] "
        + Op.op2StrWithoutAndOr(filter.getOp())
        + " r[\""
        + pathB
        + "\"]";
  }
}
