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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.Expressions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

public class RowTransformInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private static RowTransform operator = null;

  public RowTransformInfoGenerator(RowTransform operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public ProjectExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> expressions = getExpressions(context, inputSchema);
    return new ProjectExecutor(context, inputSchema.raw(), expressions);
  }

  public List<ScalarExpression<?>> getExpressions(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> ret = new ArrayList<>();
    if (inputSchema.hasKey()) {
      ret.add(new FieldNode(inputSchema.getKeyIndex(), BatchSchema.KEY.getName()));
    }
    for (int targetIndex = 0; targetIndex < operator.getFunctionCallList().size(); targetIndex++) {
      FunctionCall functionCall = operator.getFunctionCallList().get(targetIndex);
      ScalarExpression<?> expression =
          getPhysicalExpression(context, inputSchema.raw(), functionCall);
      ret.add(expression);
    }
    return ret;
  }

  private ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, FunctionCall functionCall)
      throws ComputeException {
    Function function = functionCall.getFunction();
    FunctionParams params = functionCall.getParams();
    switch (function.getFunctionType()) {
      case System:
        if (function.getMappingType() != MappingType.RowMapping) {
          throw new UnsupportedOperationException(
              "Unsupported mapping type for row transform: " + function.getMappingType());
        }
        if (function instanceof ArithmeticExpr) {
          Expression expr = params.getExpr();
          return Expressions.getPhysicalExpression(context, inputSchema, expr);
        } else {
          throw new UnsupportedOperationException(
              "Unsupported system function: " + function.getIdentifier());
        }
      case UDF:
      default:
        throw new UnsupportedOperationException(
            "Unsupported function type: " + function.getFunctionType());
    }
  }
}
