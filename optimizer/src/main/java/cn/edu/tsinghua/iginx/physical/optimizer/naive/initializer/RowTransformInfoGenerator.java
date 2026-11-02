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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalExpressionPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.KeyExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RowTransformInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final RowTransform operator;

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
    boolean remainKey =
        operator.getFunctionCallList().stream()
            .map(FunctionCall::getParams)
            .map(FunctionParams::getExpressions)
            .flatMap(List::stream)
            .noneMatch(expression -> expression instanceof KeyExpression);
    if (remainKey && inputSchema.hasKey()) {
      ret.add(new FieldNode(inputSchema.getKeyIndex(), BatchSchema.KEY.getName()));
    }
    for (int targetIndex = 0; targetIndex < operator.getFunctionCallList().size(); targetIndex++) {
      FunctionCall functionCall = operator.getFunctionCallList().get(targetIndex);
      ScalarExpression<?> expression =
          PhysicalExpressionPlannerUtils.getPhysicalExpressionOfFunctionCall(
              context, inputSchema.raw(), functionCall, true);
      ret.add(expression);
    }
    return ret;
  }
}
