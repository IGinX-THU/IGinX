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
package cn.edu.tsinghua.iginx.relational.tools;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.stream.Collectors;

public class FilterTransformer {

  AbstractRelationalMeta relationalMeta;

  public FilterTransformer(AbstractRelationalMeta relationalMeta) {
    this.relationalMeta = relationalMeta;
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
    return (getQuotName(KEY_NAME) + " " + op + " " + filter.getValue());
  }

  private String toString(ValueFilter filter) {
    RelationSchema schema = new RelationSchema(filter.getPath(), relationalMeta.getQuote());
    String path = schema.getQuoteFullName();
    String op;
    Object value;

    switch (filter.getOp()) {
      case LIKE:
      case LIKE_AND:
        op = relationalMeta.getRegexpOp();
        value = "'" + filter.getValue().getBinaryVAsString() + "$" + "'";
        break;
      case NOT_LIKE:
      case NOT_LIKE_AND:
        op = relationalMeta.getNotRegexpOp();
        value = "'" + filter.getValue().getBinaryVAsString() + "$" + "'";
        break;
      default:
        // postgresql does not support "==" but uses "=" instead
        op = Op.op2StrWithoutAndOr(filter.getOp()).replace("==", "=");
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
    RelationSchema schemaA = new RelationSchema(filter.getPathA(), relationalMeta.getQuote());
    RelationSchema schemaB = new RelationSchema(filter.getPathB(), relationalMeta.getQuote());
    String pathA = schemaA.getQuoteFullName();
    String pathB = schemaB.getQuoteFullName();

    String op =
        Op.op2StrWithoutAndOr(filter.getOp())
            .replace("==", "="); // postgresql does not support "==" but uses "=" instead

    return pathA + " " + op + " " + pathB;
  }

  private String toString(InFilter filter) {
    RelationSchema schema = new RelationSchema(filter.getPath(), relationalMeta.getQuote());
    String path = schema.getQuoteFullName();
    String op = filter.getInOp().isNotOp() ? "not in" : "in";
    String values =
        "("
            + filter.getValues().stream()
                .map(
                    value -> {
                      if (value.getDataType() == DataType.BINARY) {
                        return "'" + value.getBinaryVAsString() + "'";
                      } else {
                        return value.getValue().toString();
                      }
                    })
                .collect(Collectors.joining(","))
            + ")";
    return path + " " + op + " " + values;
  }

  private String getQuotName(String name) {
    return relationalMeta.getQuote() + name + relationalMeta.getQuote();
  }
}
