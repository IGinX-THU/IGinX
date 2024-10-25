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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.GroupsAggregateUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;

public class GroupsAggregateInfoGenerator implements UnaryExecutorFactory<GroupsAggregateUnaryExecutor> {

  private final GroupBy groupBy;

  public GroupsAggregateInfoGenerator(GroupBy groupBy) {
    this.groupBy = Objects.requireNonNull(groupBy);
  }

  @Override
  public GroupsAggregateUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> groupKeyExpressions =
        generateGroupKeyExpressions(groupBy.getGroupByCols(), inputSchema);
    List<Pair<Accumulation, Integer>> aggregateInfo =
        AggregateInfoGenerator.generateAggregateInfo(
            context, inputSchema, groupBy.getFunctionCallList());

    HashMap<Integer, Integer> groupValueIndexesMap = new HashMap<>();
    List<Integer> groupValueIndexes = new ArrayList<>();
    for (Pair<Accumulation, Integer> pair : aggregateInfo) {
      int indexInInput = pair.getV();
      if (groupValueIndexesMap.containsKey(pair.getV())) {
        continue;
      }
      groupValueIndexesMap.put(indexInInput, groupValueIndexes.size());
      groupValueIndexes.add(indexInInput);
    }
    List<ScalarExpression<?>> groupValueExpressions =
        groupValueIndexes.stream().map(FieldNode::new).collect(Collectors.toList());

    List<ExpressionAccumulation> accumulations = new ArrayList<>();
    for (Pair<Accumulation, Integer> pair : aggregateInfo) {
      Accumulation acc = pair.getK();
      int indexInInput = pair.getV();
      int indexInGroupValues = groupValueIndexesMap.get(indexInInput);
      ExpressionAccumulation exprAcc =
          new ExpressionAccumulation(
              acc, Collections.singletonList(new FieldNode(indexInGroupValues)));
      accumulations.add(exprAcc);
    }

    Schema groupValueSchema =
        ScalarExpressions.getOutputSchema(
            context.getAllocator(), groupValueExpressions, inputSchema.raw());

    List<ExpressionAccumulator> accumulators = new ArrayList<>();
    for (ExpressionAccumulation accumulation : accumulations) {
      accumulators.add(accumulation.accumulate(context.getAllocator(), groupValueSchema));
    }

    return new GroupsAggregateUnaryExecutor(
        context, inputSchema, groupKeyExpressions, groupValueExpressions, accumulators);
  }

  private static List<ScalarExpression<?>> generateGroupKeyExpressions(
      List<String> groupByCols, BatchSchema inputSchema) {
    List<Integer> groupKeyIndexes = Schemas.matchPattern(inputSchema.raw(), groupByCols);
    return groupKeyIndexes.stream().map(FieldNode::new).collect(Collectors.toList());
  }
}
