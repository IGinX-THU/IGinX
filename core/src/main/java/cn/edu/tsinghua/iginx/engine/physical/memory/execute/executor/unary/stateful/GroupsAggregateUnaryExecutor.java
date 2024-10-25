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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.group.GroupTable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GroupsAggregateUnaryExecutor extends StatefulUnaryExecutor {

  private final List<ScalarExpression<?>> groupKeyExpressions;
  private final List<ScalarExpression<?>> groupValueExpressions;
  private final List<ExpressionAccumulator> accumulators;
  private final GroupTable.Builder groupTableBuilder;
  private GroupTable groupTable = null;
  private int groupIndex = 0;

  public GroupsAggregateUnaryExecutor(
      ExecutorContext context,
      BatchSchema inputSchema,
      List<? extends ScalarExpression<?>> groupKeyExpressions,
      List<? extends ScalarExpression<?>> groupValueExpressions,
      List<ExpressionAccumulator> accumulators)
      throws ComputeException {
    super(context, inputSchema);
    this.groupKeyExpressions = new ArrayList<>(groupKeyExpressions);
    this.groupValueExpressions = new ArrayList<>(groupValueExpressions);
    this.accumulators = Objects.requireNonNull(accumulators);
    this.groupTableBuilder =
        new GroupTable.Builder(
            context.getAllocator(),
            context.getMaxBatchRowCount(),
            inputSchema.raw(),
            groupKeyExpressions,
            groupValueExpressions,
            accumulators);
  }

  @Override
  protected String getInfo() {
    return "Group"
        + groupValueExpressions
        + "By"
        + groupKeyExpressions
        + "Aggregate"
        + accumulators;
  }

  @Override
  public void close() throws ComputeException {
    groupTableBuilder.close();
    if (groupTable != null) {
      groupTable.close();
    }
  }

  @Override
  protected void internalConsume(Batch batch) throws ComputeException {
    groupTableBuilder.add(batch.raw());
  }

  @Override
  protected void internalFinish() throws ComputeException {
    groupTable = groupTableBuilder.build();
  }

  @Override
  public boolean canProduce() throws ComputeException {
    return groupTable != null && groupIndex < groupTable.getGroups().size();
  }

  @Override
  protected Batch internalProduce() throws ComputeException {
    return new Batch(groupTable.getGroups().get(groupIndex++));
  }

  @Override
  public BatchSchema getOutputSchema() throws ComputeException {
    if (groupTable == null) {
      throw new ComputeException("Cannot get output schema before finish");
    }
    return BatchSchema.of(groupTable.getSchema());
  }
}
