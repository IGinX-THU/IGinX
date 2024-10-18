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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulators;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseables;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class AggregateExecutor extends UnarySinkExecutor {

  private final List<ExpressionAccumulator> accumulators;
  private final BatchSchema outputSchema;
  private final List<Accumulator.State> states;

  public AggregateExecutor(
      ExecutorContext context, BatchSchema inputSchema, List<ExpressionAccumulator> accumulators)
      throws ComputeException {
    super(context, inputSchema);
    this.accumulators = new ArrayList<>(accumulators);
    Schema outputSchema = ExpressionAccumulators.getOutputSchema(accumulators);
    this.outputSchema = BatchSchema.of(outputSchema);
    this.states = new ArrayList<>(accumulators.size());
    try {
      for (ExpressionAccumulator acc : accumulators) {
        states.add(acc.createState());
      }
    } catch (ComputeException e) {
      close();
      throw e;
    }
  }

  @Override
  public String getInfo() {
    return "Aggregate" + accumulators;
  }

  @Override
  public BatchSchema getOutputSchema() {
    return outputSchema;
  }

  @Override
  protected void internalConsume(@WillNotClose Batch batch) throws ComputeException {
    ExpressionAccumulators.update(accumulators, states, batch.raw());
  }

  private Batch result;

  @Override
  protected void internalFinish() throws ComputeException {
    List<List<Accumulator.State>> states =
        this.states.stream().map(Collections::singletonList).collect(Collectors.toList());
    try (VectorSchemaRoot root = ExpressionAccumulators.evaluateSafe(accumulators, states)) {
      VectorSchemaRoot unnest = PhysicalFunctions.unnest(context.getAllocator(), root);
      result = new Batch(unnest);
    }
  }

  @Override
  public boolean canProduce() {
    return result != null;
  }

  @Override
  protected Batch internalProduce() {
    Batch out = result;
    result = null;
    return out;
  }

  @Override
  public void close() throws ComputeException {
    if (result != null) {
      result.close();
    }
    ComputingCloseables.close(states);
  }
}
