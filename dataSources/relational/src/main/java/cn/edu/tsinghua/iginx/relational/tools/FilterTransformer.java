/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.relational.tools;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.isLikeOp;
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

  private String toString(InFilter filter) {
    RelationSchema schema = new RelationSchema(filter.getPath(), relationalMeta.getQuote());
    String path = schema.getQuoteFullName();
    String op = filter.getInOp().toString().substring(1);
    String values =
        "("
            + filter.getValues().stream()
                .map(Value::getValue)
                .map(Object::toString)
                .collect(Collectors.joining(","))
            + ")";
    return path + " " + op + " " + values;
  }

  private String getQuotName(String name) {
    return relationalMeta.getQuote() + name + relationalMeta.getQuote();
  }
}
