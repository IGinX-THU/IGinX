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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.string.BinarySlice;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public class SubString implements RowMappingFunction {

  public static final String SUB_STRING = "substring";

  private static final SubString INSTANCE = new SubString();

  private SubString() {}

  public static SubString getInstance() {
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
    return SUB_STRING;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    checkArgs(params);

    String path = params.getPaths().get(0);
    Value valueA = row.getAsValue(path);
    if (valueA == null || valueA.isNull()) {
      return Row.EMPTY_ROW;
    }
    if (valueA.getDataType() != DataType.BINARY) {
      throw new IllegalArgumentException("Unexpected data type for substring function.");
    }

    long start = (Long) params.getArgs().get(0);
    long length = (Long) params.getArgs().get(1);
    byte[] original = valueA.getBinaryV();
    byte[] ret = Arrays.copyOfRange(original, (int) (start - 1), (int) length);

    Header newHeader =
        new Header(
            row.getHeader().getKey(),
            Collections.singletonList(
                new Field(
                    "substring(" + path + ", " + start + ", " + length + ")", DataType.BINARY)));
    return new Row(newHeader, row.getKey(), new Object[] {ret});
  }

  private void checkArgs(FunctionParams params) {
    if (params.getPaths().size() != 1 || params.getArgs().size() != 2) {
      throw new IllegalArgumentException("Unexpected params for substring.");
    }
    if (!(params.getArgs().get(0) instanceof Long)) {
      throw new IllegalArgumentException("The 2nd arg 'start' for substring should be a number.");
    }
    if (!(params.getArgs().get(1) instanceof Long)) {
      throw new IllegalArgumentException("The 3rd arg 'length' for substring should be a number.");
    }
  }

  @Override
  public ScalarExpression<?> transform(
      ExecutorContext context, Schema schema, FunctionParams params, boolean setAlias)
      throws ComputeException {
    checkArgs(params);

    String path = params.getPaths().get(0);
    List<ScalarExpression<?>> inputs =
        PhysicalExpressionPlannerUtils.getRowMappingFunctionArgumentExpressions(
            context, schema, params, false);
    long start = (Long) params.getArgs().get(0);
    long length = (Long) params.getArgs().get(1);
    int startFrom0 = (int) start - 1;

    return new CallNode<>(
        new BinarySlice(startFrom0, (int) length - startFrom0),
        setAlias ? "substring(" + path + ", " + start + ", " + length + ")" : null,
        inputs);
  }
}
