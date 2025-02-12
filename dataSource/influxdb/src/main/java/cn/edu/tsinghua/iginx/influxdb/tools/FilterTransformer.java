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
package cn.edu.tsinghua.iginx.influxdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    return "not " + toString(filter.getChild());
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
    String value = valueToString(filter.getValue());

    switch (filter.getOp()) {
      case LIKE:
      case LIKE_AND:
        // SQL的正则匹配需要全部匹配，但InfluxDB可以部分匹配，所以需要在最后加上$以保证匹配全部字符串。
        return "r[\"" + path + "\"]  =~ /" + filter.getValue().getBinaryVAsString() + "$/";
      case NOT_LIKE:
      case NOT_LIKE_AND:
        return "r[\"" + path + "\"]  !~ /" + filter.getValue().getBinaryVAsString() + "$/";
      default:
        return "r[\"" + path + "\"] " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + value;
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

  private static String toString(InFilter filter) {
    Set<Value> valueSet = filter.getValues();
    List<Filter> filters = new ArrayList<>();

    for (Value value : valueSet) {
      filters.add(new ValueFilter(filter.getPath(), Op.E, value));
    }
    if (filter.getInOp().isNotOp()) {
      return toString(new NotFilter(new OrFilter(filters)));
    }

    return toString(new OrFilter(filters));
  }

  private static String valueToString(Value value) {
    if (value.getDataType() == DataType.BINARY) {
      return "\"" + value.getBinaryVAsString() + "\"";
    }
    return value.toString();
  }
}
