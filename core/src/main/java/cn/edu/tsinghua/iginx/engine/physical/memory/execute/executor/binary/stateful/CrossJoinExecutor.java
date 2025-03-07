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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinArrayList;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This class is used to execute hash join operation. Left is the build side, and right is the probe
 * side.
 */
public class CrossJoinExecutor extends StatefulBinaryExecutor {

  private final List<ScalarExpression<?>> outputExpressions;

  private final JoinArrayList.Builder joinArrayListBuilder;
  private JoinArrayList joinArrayList;
  private Schema outputSchema;

  public CrossJoinExecutor(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      List<ScalarExpression<?>> outputExpressions) {
    super(context, leftSchema, rightSchema, 1);
    this.outputExpressions = new ArrayList<>(outputExpressions);
    this.joinArrayListBuilder =
        new JoinArrayList.Builder(context.getAllocator(), getLeftSchema().raw());
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      List<Field> outputFields = new ArrayList<>();
      outputFields.addAll(leftSchema.raw().getFields());
      outputFields.addAll(rightSchema.raw().getFields());
      this.outputSchema =
          ScalarExpressionUtils.getOutputSchema(
              context.getAllocator(), outputExpressions, new Schema(outputFields));
    }
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return "CrossJoin output " + outputExpressions;
  }

  @Override
  public void close() throws ComputeException {
    joinArrayListBuilder.close();
    if (joinArrayList != null) {
      joinArrayList.close();
    }
    super.close();
  }

  @Override
  public boolean needConsumeRight() throws ComputeException {
    return super.needConsumeRight() && joinArrayList != null;
  }

  @Override
  protected void consumeLeftUnchecked(Batch batch) throws ComputeException {
    joinArrayListBuilder.add(batch.flattened(context.getAllocator()));
  }

  @Override
  protected void consumeLeftEndUnchecked() throws ComputeException {
    joinArrayList =
        joinArrayListBuilder.build(
            context.getAllocator(),
            outputExpressions,
            getRightSchema().raw(),
            (dictionaryProvider, data, selection) ->
                offerResult(Batch.of(data, dictionaryProvider, selection)));
  }

  @Override
  protected void consumeRightUnchecked(Batch batch) throws ComputeException {
    try (VectorSchemaRoot batchFlattened = batch.flattened(context.getAllocator())) {
      joinArrayList.probe(batchFlattened);
    }
  }

  @Override
  protected void consumeRightEndUnchecked() throws ComputeException {}
}
