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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinHashMap;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinType;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class is used to execute hash join operation. Left is the build side, and right is the probe
 * side.
 */
public class HashJoinExecutor extends StatefulBinaryExecutor {

  private final JoinType joinType;
  private final List<ScalarExpression<?>> leftOutputExpressions;
  private final List<ScalarExpression<?>> rightOutputExpressions;
  private final List<ScalarExpression<?>> leftKeyExpressions;
  private final List<ScalarExpression<?>> rightKeyExpressions;

  private final Schema outputSchema;
  private final JoinHashMap.Builder joinHashMapBuilder;
  private JoinHashMap joinHashMap;
  private boolean finished = false;

  public HashJoinExecutor(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      JoinType joinType,
      List<ScalarExpression<?>> leftOutputExpressions,
      List<ScalarExpression<?>> rightOutputExpressions,
      List<Pair<ScalarExpression<?>, ScalarExpression<?>>> on)
      throws ComputeException {
    super(context, leftSchema, rightSchema);
    this.joinType = Objects.requireNonNull(joinType);
    this.leftOutputExpressions = new ArrayList<>(leftOutputExpressions);
    this.rightOutputExpressions = new ArrayList<>(rightOutputExpressions);
    this.leftKeyExpressions = on.stream().map(Pair::getLeft).collect(Collectors.toList());
    this.rightKeyExpressions = on.stream().map(Pair::getRight).collect(Collectors.toList());

    this.outputSchema =
        Schemas.merge(
            ScalarExpressions.getOutputSchema(
                context.getAllocator(), leftOutputExpressions, getLeftSchema().raw()),
            ScalarExpressions.getOutputSchema(
                context.getAllocator(), rightOutputExpressions, getRightSchema().raw()));

    this.joinHashMapBuilder =
        new JoinHashMap.Builder(
            context.getAllocator(),
            getLeftSchema().raw(),
            leftKeyExpressions,
            leftOutputExpressions);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return "HashJoin("
        + joinType
        + ")"
        + leftOutputExpressions
        + "And"
        + rightOutputExpressions
        + "On"
        + leftKeyExpressions
        + "Equal"
        + rightKeyExpressions;
  }

  @Override
  public void close() throws ComputeException {
    joinHashMapBuilder.close();
    if (joinHashMap != null) {
      joinHashMap.close();
    }
  }

  @Override
  public boolean needConsumeLeft() throws ComputeException {
    return joinHashMap == null;
  }

  @Override
  public boolean needConsumeRight() throws ComputeException {
    return joinHashMap != null && joinHashMap.getOutput().isEmpty() && !finished;
  }

  @Override
  public void consumeLeft(VectorSchemaRoot batch) throws ComputeException {
    Preconditions.checkState(needConsumeLeft());
    if (batch.getRowCount() != 0) {
      joinHashMapBuilder.add(batch);
      return;
    }
    joinHashMap =
        joinHashMapBuilder.build(
            context.getBatchRowCount(),
            getRightSchema().raw(),
            rightKeyExpressions,
            rightOutputExpressions);
  }

  @Override
  public void consumeRight(VectorSchemaRoot batch) throws ComputeException {
    Preconditions.checkState(needConsumeRight());
    if (batch.getRowCount() != 0) {
      joinHashMap.probe(batch, joinType.needOutputRightUnmatched());
    }
    finished = true;
    if (joinType.needOutputLeftUnmatched()) {
      joinHashMap.outputBuildSideUnmatched();
    }
    joinHashMap.getOutput().flush();
  }

  @Override
  public VectorSchemaRoot produce() throws ComputeException {
    Preconditions.checkState(!needConsumeLeft());
    Preconditions.checkState(!needConsumeRight());
    if (finished) {
      return VectorSchemaRoot.create(outputSchema, context.getAllocator());
    }
    return joinHashMap.getOutput().poll();
  }
}
