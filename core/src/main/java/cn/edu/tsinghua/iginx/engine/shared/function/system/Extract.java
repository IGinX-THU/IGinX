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
package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.temporal.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalExpressionPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.apache.arrow.vector.types.pojo.Schema;

public class Extract implements RowMappingFunction {

  public static final String EXTRACT = "extract";

  private static final Extract INSTANCE = new Extract();

  private Extract() {}

  public static Extract getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.RowMapping;
  }

  @Override
  public String getIdentifier() {
    return EXTRACT;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (params.getPaths().size() != 1 || params.getArgs().size() != 1) {
      throw new IllegalArgumentException("Unexpected params for extract.");
    }

    String path = params.getPaths().get(0);
    Value valueA = row.getAsValue(path);
    if (valueA == null || valueA.isNull()) {
      return Row.EMPTY_ROW;
    }
    if (valueA.getDataType() != DataType.LONG && valueA.getDataType() != DataType.INTEGER) {
      throw new IllegalArgumentException("Unexpected data type for extract function.");
    }

    if (!(params.getArgs().get(0) instanceof byte[])) {
      throw new IllegalArgumentException("The 2nd arg 'field' for extract should be a string.");
    }

    long timestamp = (long) valueA.getValue();
    Instant instant = Instant.ofEpochMilli(timestamp);
    LocalDateTime localDateTime =
        instant.atZone(TimeZone.getTimeZone("GMT").toZoneId()).toLocalDateTime();

    int ret;
    String field = new String((byte[]) params.getArgs().get(0)).toLowerCase();
    switch (field) {
      case "year":
        ret = localDateTime.getYear();
        break;
      case "month":
        ret = localDateTime.getMonthValue();
        break;
      case "day":
        ret = localDateTime.getDayOfMonth();
        break;
      case "hour":
        ret = localDateTime.getHour();
        break;
      case "minute":
        ret = localDateTime.getMinute();
        break;
      case "second":
        ret = localDateTime.getSecond();
        break;
      default:
        throw new IllegalArgumentException(
            "The 2nd arg 'field' for extract is expected in [\"year\", \"month\", \"day\", \"hour\", \"minute\", \"second\"].");
    }

    Header newHeader =
        new Header(
            row.getHeader().getKey(),
            Collections.singletonList(
                new Field("extract(" + path + ", " + field + ")", DataType.INTEGER)));
    return new Row(newHeader, row.getKey(), new Object[] {ret});
  }

  @Override
  public ScalarExpression<?> transform(
      ExecutorContext context, Schema schema, FunctionParams params, boolean setAlias)
      throws ComputeException {
    List<Object> args = params.getArgs();
    List<ScalarExpression<?>> inputs =
        PhysicalExpressionPlannerUtils.getRowMappingFunctionArgumentExpressions(
            context, schema, params, false);

    if (inputs.size() != 1) {
      throw new ComputeException(
          "unexpected input path number for extract, expected 1, but got " + inputs.size());
    }
    if (args.size() != 1) {
      throw new ComputeException(
          "unexpected args number for extract, expected 1, but got " + args.size());
    }
    Object arg = args.get(0);
    if (!(arg instanceof byte[])) {
      throw new ComputeException("The arg of for extract should be a string.");
    }
    ScalarFunction<?> function;
    String functionArg = new String((byte[]) arg);
    switch (new String((byte[]) arg)) {
      case "year":
        function = new Year();
        break;
      case "month":
        function = new Month();
        break;
      case "day":
        function = new Day();
        break;
      case "hour":
        function = new Hour();
        break;
      case "minute":
        function = new Minute();
        break;
      case "second":
        function = new Second();
        break;
      default:
        throw new ComputeException(
            "The arg of for extract should be one of year, month, day, hour, minute, second, millisecond.");
    }

    Schema argumentsSchema =
        ScalarExpressionUtils.getOutputSchema(context.getAllocator(), inputs, schema);
    String outputColumnName =
        "extract(" + argumentsSchema.getFields().get(0).getName() + ", " + functionArg + ")";
    return new CallNode<>(function, setAlias ? outputColumnName : null, inputs);
  }
}
