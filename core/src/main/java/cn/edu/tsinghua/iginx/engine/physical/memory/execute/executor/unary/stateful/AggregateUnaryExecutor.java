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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulators;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseables;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class AggregateUnaryExecutor extends StatefulUnaryExecutor {

  private final List<ExpressionAccumulator> accumulators;
  private final Schema outputSchema;
  private final List<Accumulator.State> states;

  public AggregateUnaryExecutor(
      ExecutorContext context, Schema inputSchema, List<ExpressionAccumulator> accumulators)
      throws ComputeException {
    super(context, inputSchema, 1);
    this.accumulators = new ArrayList<>(accumulators);
    this.outputSchema =
        PhysicalFunctions.unnest(ExpressionAccumulators.getOutputSchema(accumulators));
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
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public void close() throws ComputeException {
    ComputingCloseables.close(states);
    states.clear();
    super.close();
  }

  @Override
  protected void consumeUnchecked(Batch batch) throws ComputeException {
    try (VectorSchemaRoot flattened = batch.flattened(context.getAllocator())) {
      ExpressionAccumulators.update(accumulators, states, flattened);
    }
  }

  @Override
  protected void consumeEndUnchecked() throws ComputeException {
    List<List<Accumulator.State>> statesColumns =
        this.states.stream().map(Collections::singletonList).collect(Collectors.toList());
    try (VectorSchemaRoot root = ExpressionAccumulators.evaluateSafe(accumulators, statesColumns);
        VectorSchemaRoot unnested = PhysicalFunctions.unnest(context.getAllocator(), root);
        Batch output = Batch.of(unnested)) {
      offerResult(output);
    }
  }
}
