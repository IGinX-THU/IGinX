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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This class is used to execute hash join operation. Left is the build side, and right is the probe
 * side.
 */
public class HashJoinExecutor extends StatefulBinaryExecutor {

  private final JoinOption joinOption;
  private final List<ScalarExpression<?>> leftOutputFieldExpressions;
  private final List<ScalarExpression<?>> rightOutputFieldExpressions;
  private final List<ScalarExpression<?>> leftOnFieldExpressions;
  private final List<ScalarExpression<?>> rightOnFieldExpressions;
  private final ScalarExpression<BitVector> onFieldsPairTester;
  private final ScalarExpression<IntVector> leftHasher;
  private final ScalarExpression<IntVector> rightHasher;

  private final Schema outputSchema;
  private final JoinHashMap.Builder joinHashMapBuilder;
  private JoinHashMap joinHashMap;

  public HashJoinExecutor(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      JoinOption joinOption,
      List<? extends ScalarExpression<?>> leftOutputFieldExpressions,
      List<? extends ScalarExpression<?>> rightOutputFieldExpressions,
      List<? extends ScalarExpression<?>> leftOnFieldExpressions,
      List<? extends ScalarExpression<?>> rightOnFieldExpressions,
      ScalarExpression<BitVector> onFieldsPairTester,
      ScalarExpression<IntVector> leftHasher,
      ScalarExpression<IntVector> rightHasher)
      throws ComputeException {
    super(context, leftSchema, rightSchema, 1);
    this.joinOption = Objects.requireNonNull(joinOption);
    this.leftOutputFieldExpressions = new ArrayList<>(leftOutputFieldExpressions);
    this.rightOutputFieldExpressions = new ArrayList<>(rightOutputFieldExpressions);
    this.leftOnFieldExpressions = new ArrayList<>(leftOnFieldExpressions);
    this.rightOnFieldExpressions = new ArrayList<>(rightOnFieldExpressions);
    this.onFieldsPairTester = Objects.requireNonNull(onFieldsPairTester);
    this.leftHasher = Objects.requireNonNull(leftHasher);
    this.rightHasher = Objects.requireNonNull(rightHasher);

    List<Field> outputFields = new ArrayList<>();
    outputFields.addAll(
        ScalarExpressions.getOutputSchema(
                context.getAllocator(), leftOutputFieldExpressions, getLeftSchema().raw())
            .getFields());
    outputFields.addAll(
        ScalarExpressions.getOutputSchema(
                context.getAllocator(), rightOutputFieldExpressions, getRightSchema().raw())
            .getFields());
    if (joinOption.needMark()) {
      outputFields.add(Field.nullable(joinOption.markColumnName(), Types.MinorType.BIT.getType()));
    }
    this.outputSchema = new Schema(outputFields);

    this.joinHashMapBuilder =
        new JoinHashMap.Builder(
            context.getAllocator(),
            getLeftSchema().raw(),
            joinOption,
            this.leftHasher,
            this.leftOnFieldExpressions,
            this.leftOutputFieldExpressions);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return "Join("
        + joinOption
        + ") "
        + leftOutputFieldExpressions
        + " x "
        + rightOutputFieldExpressions
        + " On "
        + leftOnFieldExpressions
        + " x "
        + rightOnFieldExpressions
        + " Where "
        + onFieldsPairTester
        + " And "
        + leftHasher
        + " = "
        + rightHasher;
  }

  @Override
  public void close() throws ComputeException {
    joinHashMapBuilder.close();
    if (joinHashMap != null) {
      joinHashMap.close();
    }
  }

  @Override
  public boolean needConsumeRight() throws ComputeException {
    return super.needConsumeRight() && joinHashMap != null;
  }

  @Override
  protected void consumeLeftUnchecked(VectorSchemaRoot batch) throws ComputeException {
    joinHashMapBuilder.add(batch);
  }

  @Override
  protected void consumeLeftEnd() throws ComputeException {
    joinHashMap =
        joinHashMapBuilder.build(
            context.getBatchRowCount(),
            getRightSchema().raw(),
            rightHasher,
            rightOnFieldExpressions,
            rightOutputFieldExpressions,
            onFieldsPairTester);
  }

  @Override
  protected void consumeRightUnchecked(VectorSchemaRoot batch) throws ComputeException {
    joinHashMap.probe(batch);
    while (!joinHashMap.getOutput().isEmpty()) {
      offerResult(joinHashMap.getOutput().poll());
    }
  }

  @Override
  protected void consumeRightEnd() throws ComputeException {
    if (joinOption.needOutputBuildSideUnmatched()) {
      joinHashMap.outputBuildSideUnmatched();
    }
    joinHashMap.getOutput().flush();
    while (!joinHashMap.getOutput().isEmpty()) {
      offerResult(joinHashMap.getOutput().poll());
    }
  }
}
