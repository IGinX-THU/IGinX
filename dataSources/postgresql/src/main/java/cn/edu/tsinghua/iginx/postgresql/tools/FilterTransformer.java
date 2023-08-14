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
package cn.edu.tsinghua.iginx.postgresql.tools;

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.*;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.stream.Collectors;

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
      case Bool:
        return toString((BoolFilter) filter);
      default:
        return "";
    }
  }

  private static String toString(AndFilter filter) {
    return filter.getChildren().stream()
        .map(FilterTransformer::toString)
        .collect(Collectors.joining(" and ", "(", ")"));
  }

  private static String toString(BoolFilter filter) {
    return filter.isTrue() ? "true" : "false";
  }

  private static String toString(NotFilter filter) {
    return "not " + toString(filter.getChild());
  }

  private static String toString(KeyFilter filter) {
    String op =
        Op.op2Str(filter.getOp())
            .replace("==", "="); // postgresql does not support "==" but uses "=" instead
    return (KEY_NAME + " " + op + " " + filter.getValue())
        .replace(IGINX_SEPARATOR, Constants.POSTGRESQL_SEPARATOR);
  }

  private static String toString(ValueFilter filter) {
    int lastIndexOfSeparator = filter.getPath().lastIndexOf(IGINX_SEPARATOR);
    String path =
        filter
                .getPath()
                .substring(0, lastIndexOfSeparator)
                .replace(IGINX_SEPARATOR, Constants.POSTGRESQL_SEPARATOR)
            + filter.getPath().substring(lastIndexOfSeparator);

    String op =
        filter.getOp() == Op.LIKE
            ? "~"
            : Op.op2Str(filter.getOp())
                .replace("==", "="); // postgresql does not support "==" but uses "=" instead

    String regex_symbol = filter.getOp() == Op.LIKE ? "$" : "";

    Object value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "'" + filter.getValue().getBinaryVAsString() + regex_symbol + "'"
            : filter.getValue().getValue();

    return path + " " + op + " " + value;
  }

  private static String toString(OrFilter filter) {
    return filter.getChildren().stream()
        .map(FilterTransformer::toString)
        .collect(Collectors.joining(" or ", "(", ")"));
  }

  private static String toString(PathFilter filter) {
    int lastIndexOfSeparatorA = filter.getPathA().lastIndexOf(IGINX_SEPARATOR);
    int lastIndexOfSeparatorB = filter.getPathB().lastIndexOf(IGINX_SEPARATOR);
    String pathA =
        filter
                .getPathA()
                .substring(0, lastIndexOfSeparatorA)
                .replace(IGINX_SEPARATOR, Constants.POSTGRESQL_SEPARATOR)
            + filter.getPathA().substring(lastIndexOfSeparatorA);
    String pathB =
        filter
                .getPathB()
                .substring(0, lastIndexOfSeparatorB)
                .replace(IGINX_SEPARATOR, Constants.POSTGRESQL_SEPARATOR)
            + filter.getPathB().substring(lastIndexOfSeparatorB);

    String op =
        Op.op2Str(filter.getOp())
            .replace("==", "="); // postgresql does not support "==" but uses "=" instead
    return pathA + " " + op + " " + pathB;
  }
}
