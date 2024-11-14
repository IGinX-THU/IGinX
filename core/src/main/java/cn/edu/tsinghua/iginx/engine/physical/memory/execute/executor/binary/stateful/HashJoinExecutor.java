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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
  private final PredicateExpression matcher;
  private final List<ScalarExpression<?>> outputExpressions;
  private final ScalarExpression<IntVector> probeSideHasher;

  private final JoinHashMap.Builder joinHashMapBuilder;
  private JoinHashMap joinHashMap;
  private Schema outputSchema;

  public HashJoinExecutor(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      JoinOption joinOption,
      PredicateExpression matcher,
      List<ScalarExpression<?>> outputExpressions,
      ScalarExpression<IntVector> leftHasher,
      ScalarExpression<IntVector> rightHasher) {
    super(context, leftSchema, rightSchema, 1);
    this.joinOption = Objects.requireNonNull(joinOption);
    this.matcher = Objects.requireNonNull(matcher);
    this.outputExpressions = new ArrayList<>(outputExpressions);
    this.probeSideHasher = Objects.requireNonNull(rightHasher);
    this.joinHashMapBuilder =
        new JoinHashMap.Builder(context.getAllocator(), leftHasher, getLeftSchema().raw());
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      List<Field> outputFields = new ArrayList<>();
      outputFields.addAll(leftSchema.raw().getFields());
      outputFields.addAll(rightSchema.raw().getFields());
      outputFields.add(Field.nullable("mark", Types.MinorType.BIT.getType()));
      this.outputSchema =
          ScalarExpressions.getOutputSchema(
              context.getAllocator(), outputExpressions, new Schema(outputFields));
    }
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return "HashJoin(" + joinOption + ") on " + matcher + " output " + outputExpressions;
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
  protected void consumeLeftUnchecked(Batch batch) throws ComputeException {
    joinHashMapBuilder.add(batch.flattened(context.getAllocator()));
  }

  @Override
  protected void consumeLeftEnd() throws ComputeException {
    joinHashMap =
        joinHashMapBuilder.build(
            context.getAllocator(),
            joinOption,
            matcher,
            outputExpressions,
            probeSideHasher,
            getRightSchema().raw(),
            (dictionaryProvider, data) -> offerResult(Batch.of(data, dictionaryProvider)));
  }

  @Override
  protected void consumeRightUnchecked(Batch batch) throws ComputeException {
    try (VectorSchemaRoot batchFlattened = batch.flattened(context.getAllocator())) {
      joinHashMap.probe(batchFlattened);
    }
  }

  @Override
  protected void consumeRightEnd() throws ComputeException {
    joinHashMap.flush();
  }
}
