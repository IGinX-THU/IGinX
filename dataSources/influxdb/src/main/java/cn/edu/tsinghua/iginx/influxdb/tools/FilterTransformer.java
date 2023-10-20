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
      default:
        return "";
    }
  }

  private static String toString(AndFilter filter) {
    return filter.getChildren().stream()
        .map(FilterTransformer::toString)
        .collect(Collectors.joining(" and ", "(", ")"));
  }

  private static String toString(NotFilter filter) {
    return "not " + filter.toString();
  }

  private static String toString(KeyFilter filter) {
    return "r[\"_time\"] " + Op.op2Str(filter.getOp()) + " time(v: " + filter.getValue() + ")";
  }

  private static String toString(ValueFilter filter) {
    // path 获取的是 table.field，需要删掉.前面的table名。
    InfluxDBSchema schema = new InfluxDBSchema(filter.getPath());
    String path = schema.getField();
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

    return "r[\"" + path + "\"] " + op2StrWithoutAnd(filter.getOp()) + " " + value;
  }

  private static String toString(OrFilter filter) {
    return filter.getChildren().stream()
        .map(FilterTransformer::toString)
        .collect(Collectors.joining(" or ", "(", ")"));
  }

  private static String toString(PathFilter filter) {
    // path 获取的是 table.field，需要删掉.前面的table名。
    InfluxDBSchema schemaA = new InfluxDBSchema(filter.getPathA());
    InfluxDBSchema schemaB = new InfluxDBSchema(filter.getPathB());
    String pathA = schemaA.getField();
    String pathB = schemaB.getField();

    return "r[\"" + pathA + "\"] " + op2StrWithoutAnd(filter.getOp()) + " r[\"" + pathB + "\"]";
  }

  private static String op2StrWithoutAnd(Op op) {
    String opStr = Op.op2Str(op);
    if (opStr.startsWith("&")) {
      return opStr.substring(1);
    }
    return opStr;
  }
}
