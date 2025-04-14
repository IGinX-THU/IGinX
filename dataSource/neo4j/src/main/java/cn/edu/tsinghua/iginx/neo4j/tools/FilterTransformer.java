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
package cn.edu.tsinghua.iginx.neo4j.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.stream.Collectors;

public class FilterTransformer {

  private String key;
  private String label;

  public FilterTransformer(String key, String label) {
    this.key = key;
    this.label = label;
  }

  public String toString(Filter filter) {
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
      case Bool:
        return toString((BoolFilter) filter);
      case In:
        return toString((InFilter) filter);
      default:
        return "";
    }
  }

  private String toString(AndFilter filter) {
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

  private String toString(BoolFilter filter) {
    return filter.isTrue() ? "true" : "false";
  }

  private String toString(NotFilter filter) {
    return "not " + toString(filter.getChild());
  }

  private String toString(KeyFilter filter) {
    String op =
        Op.op2StrWithoutAndOr(filter.getOp())
            .replace("==", "="); // postgresql does not support "==" but uses "=" instead
    return (getQuotName(key) + " " + op + " " + filter.getValue());
  }

  private String toString(ValueFilter filter) {
    String path = filter.getPath();
    String op;
    Object value;

    op = Op.op2StrWithoutAndOr(filter.getOp()).replace("==", "=");
    value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "'" + filter.getValue().getBinaryVAsString() + "'"
            : filter.getValue().getValue();

    Neo4jSchema schema = new Neo4jSchema(path);
    if (!schema.getLabelName().equals(this.label)) {
      return "";
    }

    return schema.getFullName() + " " + op + " " + value;
  }

  private String toString(OrFilter filter) {
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

  private String toString(PathFilter filter) {
    String op =
        Op.op2StrWithoutAndOr(filter.getOp())
            .replace("==", "="); // postgresql does not support "==" but uses "=" instead

    Neo4jSchema schemaA = new Neo4jSchema(filter.getPathA());
    Neo4jSchema schemaB = new Neo4jSchema(filter.getPathB());
    if (!schemaA.getLabelName().equals(this.label) || !schemaB.getLabelName().equals(this.label)) {
      return "";
    }
    return new Neo4jSchema(filter.getPathA()).getFullName()
        + " "
        + op
        + " "
        + new Neo4jSchema(filter.getPathB()).getFullName();
  }

  private String toString(InFilter filter) {
    String path = filter.getPath();

    String op = filter.getInOp().isNotOp() ? "not in" : "in";
    String values =
        "["
            + filter.getValues().stream()
                .map(
                    value -> {
                      if (value.getDataType() == DataType.BINARY) {
                        return "'" + value.getBinaryVAsString() + "'";
                      } else {
                        return value.getValue();
                      }
                    })
                .map(Object::toString)
                .collect(Collectors.joining(","))
            + "]";

    Neo4jSchema schema = new Neo4jSchema(path);
    if (!schema.getLabelName().equals(this.label)) {
      return "";
    }
    return (filter.getInOp().isNotOp() ? "not " : "")
        + new Neo4jSchema(path).getFullName()
        + " in "
        + values;
  }

  private String getQuotName(String name) {
    return name;
  }
}
