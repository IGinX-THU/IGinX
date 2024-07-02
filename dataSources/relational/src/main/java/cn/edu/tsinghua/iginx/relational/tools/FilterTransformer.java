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
package cn.edu.tsinghua.iginx.relational.tools;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.isLikeOp;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;

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

    String op =
        isLikeOp(filter.getOp())
            ? relationalMeta.getRegexpOp()
            : Op.op2StrWithoutAndOr(filter.getOp())
                .replace("==", "="); // postgresql does not support "==" but uses "=" instead

    String regexSymbol = isLikeOp(filter.getOp()) ? "$" : "";

    Object value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "'" + filter.getValue().getBinaryVAsString() + regexSymbol + "'"
            : filter.getValue().getValue();

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

  private String getQuotName(String name) {
    return relationalMeta.getQuote() + name + relationalMeta.getQuote();
  }
}
