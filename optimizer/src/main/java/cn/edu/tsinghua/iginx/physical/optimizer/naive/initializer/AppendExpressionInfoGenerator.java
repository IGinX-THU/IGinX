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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalExpressionPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import java.util.ArrayList;
import java.util.List;

public class AppendExpressionInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final List<Expression> appendExpressions;

  public AppendExpressionInfoGenerator(List<Expression> appendExpressions) {
    this.appendExpressions = new ArrayList<>(appendExpressions);
  }

  @Override
  public ProjectExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> scalarExpressions = new ArrayList<>();
    for (int i = 0; i < inputSchema.getFieldCount(); i++) {
      scalarExpressions.add(new FieldNode(i));
    }
    for (Expression expression : appendExpressions) {
      String columnName = expression.getColumnName();
      List<Integer> matchedIndices = Schemas.matchPatternIgnoreKey(inputSchema.raw(), columnName);
      if (!matchedIndices.isEmpty()) {
        continue;
      }
      ScalarExpression<?> scalarExpression =
          PhysicalExpressionPlannerUtils.getPhysicalExpression(
              context, inputSchema.raw(), expression, true);
      scalarExpressions.add(scalarExpression);
    }
    return new ProjectExecutor(context, inputSchema.raw(), scalarExpressions);
  }
}
