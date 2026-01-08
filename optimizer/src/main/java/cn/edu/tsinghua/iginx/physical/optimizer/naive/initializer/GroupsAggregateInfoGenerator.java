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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.GroupsAggregateUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;

public class GroupsAggregateInfoGenerator
    implements UnaryExecutorFactory<GroupsAggregateUnaryExecutor> {

  private final GroupBy groupBy;

  public GroupsAggregateInfoGenerator(GroupBy groupBy) {
    this.groupBy = Objects.requireNonNull(groupBy);
  }

  @Override
  public GroupsAggregateUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> groupKeyExpressions =
        generateGroupKeyExpressions(groupBy.getGroupByCols(), inputSchema);
    List<ExpressionAccumulation> aggregateInfos =
        AggregateInfoGenerator.generateAggregateInfo(
            context, inputSchema, groupBy.getFunctionCallList());

    HashMap<ScalarExpression<?>, Integer> groupValueIndexesMap = new HashMap<>();
    List<ScalarExpression<?>> groupValueExpressions = new ArrayList<>();
    for (ExpressionAccumulation aggregateInfo : aggregateInfos) {
      List<? extends ScalarExpression<?>> groupValues = aggregateInfo.getExpressions();
      for (ScalarExpression<?> groupValue : groupValues) {
        if (groupValueIndexesMap.containsKey(groupValue)) {
          continue;
        }
        groupValueIndexesMap.put(groupValue, groupValueExpressions.size());
        groupValueExpressions.add(groupValue);
      }
    }

    Schema groupValueSchema =
        ScalarExpressionUtils.getOutputSchema(
            context.getAllocator(), groupValueExpressions, inputSchema.raw());

    List<ExpressionAccumulation> accumulations = new ArrayList<>();
    for (ExpressionAccumulation aggregateInfo : aggregateInfos) {
      List<FieldNode> groupIndex = new ArrayList<>();
      for (ScalarExpression<?> groupValue : aggregateInfo.getExpressions()) {
        groupIndex.add(new FieldNode(groupValueIndexesMap.get(groupValue)));
      }
      accumulations.add(new ExpressionAccumulation(aggregateInfo.getAccumulation(), groupIndex));
    }

    List<ExpressionAccumulator> accumulators = new ArrayList<>();
    for (ExpressionAccumulation accumulation : accumulations) {
      accumulators.add(accumulation.accumulate(context.getAllocator(), groupValueSchema));
    }

    return new GroupsAggregateUnaryExecutor(
        context, inputSchema.raw(), groupKeyExpressions, groupValueExpressions, accumulators);
  }

  private static List<ScalarExpression<?>> generateGroupKeyExpressions(
      List<String> groupByCols, BatchSchema inputSchema) {
    List<Integer> groupKeyIndexes = Schemas.matchPatternIgnoreKey(inputSchema.raw(), groupByCols);
    return groupKeyIndexes.stream().map(FieldNode::new).collect(Collectors.toList());
  }
}
