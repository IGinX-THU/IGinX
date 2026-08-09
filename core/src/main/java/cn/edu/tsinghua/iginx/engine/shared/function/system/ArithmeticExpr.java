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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalExpressionPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArithmeticExpr implements RowMappingFunction {

  public static final String ARITHMETIC_EXPR = "arithmetic_expr";

  private static final ArithmeticExpr INSTANCE = new ArithmeticExpr();

  private ArithmeticExpr() {}

  public static ArithmeticExpr getInstance() {
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
    return ARITHMETIC_EXPR;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (params.getExpressions().size() != 1) {
      throw new IllegalArgumentException("unexpected params for arithmetic_expr.");
    }
    Expression expr = params.getExpression(0);

    Value ret = ExprUtils.calculateExpr(row, expr);
    if (ret == null) {
      return Row.EMPTY_ROW;
    }

    Field targetField = new Field(expr.getColumnName(), ret.getDataType());

    Header header =
        row.getHeader().hasKey()
            ? new Header(Field.KEY, Collections.singletonList(targetField))
            : new Header(Collections.singletonList(targetField));

    return new Row(header, row.getKey(), new Object[] {ret.getValue()});
  }

  @Override
  public ScalarExpression<?> transform(
      ExecutorContext context, Schema schema, FunctionParams params, boolean setAlias)
      throws ComputeException {
    List<ScalarExpression<?>> inputs =
        PhysicalExpressionPlannerUtils.getRowMappingFunctionArgumentExpressions(
            context, schema, params, setAlias);

    if (inputs.size() != 1) {
      throw new ComputeException("ArithmeticExpr call args size must be 1");
    }
    return inputs.get(0);
  }
}
