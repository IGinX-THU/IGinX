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

import static cn.edu.tsinghua.iginx.neo4j.tools.Constants.IDENTITY_PROPERTY_NAME;
import static cn.edu.tsinghua.iginx.neo4j.tools.Neo4jSchema.getQuoteName;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class FilterTransformer {

  private String key;

  private String storageUnit;
  private String label;

  public FilterTransformer(String key, String storageUnit, String label) {
    this.key = key;
    this.storageUnit = storageUnit;
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
            .replace("==", "=")
            .replace("!=", "<>"); // postgresql does not support "==" but uses "=" instead

    if (key.startsWith("id(") && key.endsWith(")")) {
      return key + " " + op + " " + filter.getValue();
    }
    return getQuotName(key) + " " + op + " " + filter.getValue();
  }

  private String getFullPath(String path) {
    if (StringUtils.isNotEmpty(storageUnit) && !path.startsWith(IDENTITY_PROPERTY_NAME)) {
      if (!path.startsWith(storageUnit) && !path.startsWith("`" + storageUnit)) {
        if (path.startsWith("`")) {
          path = "`" + storageUnit + "." + path.substring(1);
        } else {
          path = storageUnit + "." + path;
        }
      }
    }
    return path;
  }

  private String toString(ValueFilter filter) {
    Neo4jSchema schema = new Neo4jSchema(filter.getPath());

    if (StringUtils.isNotEmpty(label)
        && !schema.getLabelName().equals(label)
        && !schema.getLabelName().equals(storageUnit + "." + label)) {
      return "true";
    }

    String path = getFullPath(schema.getFullName());
    String op;
    Object value;

    switch (filter.getOp()) {
      case LIKE:
      case LIKE_AND:
        value = "'^" + filter.getValue().getBinaryVAsString() + "$" + "'";
        return path + " =~ " + value.toString().replace("%", ".*");
      case NOT_LIKE:
      case NOT_LIKE_AND:
        value = "'^" + filter.getValue().getBinaryVAsString() + "$" + "'";
        return " NOT " + path + " =~ " + value.toString().replace("%", ".*");
      default:
        op = Op.op2StrWithoutAndOr(filter.getOp()).replace("==", "=").replace("!=", "<>");
        value =
            filter.getValue().getDataType() == DataType.BINARY
                ? "'" + filter.getValue().getBinaryVAsString() + "'"
                : filter.getValue().getValue();
        break;
    }

    return path + " " + op + " " + value;
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
    Neo4jSchema schemaA = new Neo4jSchema(filter.getPathA());
    Neo4jSchema schemaB = new Neo4jSchema(filter.getPathB());
    if (StringUtils.isNotEmpty(label)
        && !schemaA.getLabelName().equals(label)
        && !schemaA.getLabelName().equals(storageUnit + "." + label)) {
      return "true";
    }
    if (StringUtils.isNotEmpty(label)
        && !schemaB.getLabelName().equals(label)
        && !schemaB.getLabelName().equals(storageUnit + "." + label)) {
      return "true";
    }

    String op =
        Op.op2StrWithoutAndOr(filter.getOp())
            .replace("==", "=")
            .replace("!=", "<>"); // postgresql does not support "==" but uses "=" instead

    return new Neo4jSchema(getFullPath(filter.getPathA())).getFullName()
        + " "
        + op
        + " "
        + new Neo4jSchema(getFullPath(filter.getPathB())).getFullName();
  }

  private String toString(InFilter filter) {
    Neo4jSchema schema = new Neo4jSchema(filter.getPath());
    if (StringUtils.isNotEmpty(label)
        && !schema.getLabelName().equals(label)
        && !schema.getLabelName().equals(storageUnit + "." + label)) {
      return "true";
    }
    String path = getFullPath(filter.getPath());

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

    return (filter.getInOp().isNotOp() ? "not " : "")
        + new Neo4jSchema(path).getFullName()
        + " in "
        + values;
  }

  private String getQuotName(String name) {
    return getQuoteName(name);
  }
}
