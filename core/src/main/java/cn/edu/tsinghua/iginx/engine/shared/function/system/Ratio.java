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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
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
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;

public class Ratio implements RowMappingFunction {

  public static final String RATIO = "ratio";

  private static final Ratio INSTANCE = new Ratio();

  private Ratio() {}

  public static Ratio getInstance() {
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
    return RATIO;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (params.getPaths().size() != 2) {
      throw new IllegalArgumentException("unexpected params for ratio.");
    }

    String pathA = params.getPaths().get(0);
    String pathB = params.getPaths().get(1);

    Value valueA = row.getAsValue(pathA);
    Value valueB = row.getAsValue(pathB);
    if (valueA == null || valueB == null) {
      return Row.EMPTY_ROW;
    }

    valueA = ValueUtils.transformToDouble(valueA);
    valueB = ValueUtils.transformToDouble(valueB);
    if (valueB.getDoubleV() == 0.0) {
      return Row.EMPTY_ROW;
    }

    double ret = valueA.getDoubleV() / valueB.getDoubleV();

    Header newHeader =
        row.getHeader().hasKey()
            ? new Header(
                Field.KEY,
                Collections.singletonList(
                    new Field("ratio(" + pathA + ", " + pathB + ")", DataType.DOUBLE)))
            : new Header(
                Collections.singletonList(
                    new Field("ratio(" + pathA + ", " + pathB + ")", DataType.DOUBLE)));

    return new Row(newHeader, row.getKey(), new Object[] {ret});
  }

  @Override
  public ScalarExpression<?> transform(
      ExecutorContext context, Schema schema, FunctionParams params, boolean setAlias)
      throws ComputeException {
    List<ScalarExpression<?>> inputs =
        PhysicalExpressionPlannerUtils.getRowMappingFunctionArgumentExpressions(
            context, schema, params, false);

    if (inputs.size() != 2) {
      throw new ComputeException(
          "unexpected params number for ratio, expected 2, but got " + inputs.size());
    }
    Schema argumentsSchema =
        ScalarExpressionUtils.getOutputSchema(context.getAllocator(), inputs, schema);
    String outputColumnName =
        "ratio"
            + argumentsSchema.getFields().stream()
                .map(org.apache.arrow.vector.types.pojo.Field::getName)
                .collect(Collectors.joining(", ", "(", ")"));
    return new CallNode<>(
        new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.Ratio(),
        setAlias ? outputColumnName : null,
        inputs);
  }
}
