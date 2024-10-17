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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseables;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.ProjectionExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.types.Types;

public class AggregateExecutor extends UnarySinkExecutor {

  public static class AggregateInfo {
    private final Accumulator accumulator;
    private final List<PhysicalExpression> expressions;

    public AggregateInfo(Accumulator accumulator, List<PhysicalExpression> expressions) {
      this.accumulator = accumulator;
      this.expressions = expressions;
    }

    public Accumulator getAccumulator() {
      return accumulator;
    }

    public List<PhysicalExpression> getExpressions() {
      return expressions;
    }

    @Override
    public String toString() {
      return accumulator.getName()
          + expressions.stream()
              .map(PhysicalExpression::toString)
              .collect(Collectors.joining(", ", "(", ")"));
    }
  }

  private final List<AggregateInfo> aggregateInfos;
  private final List<ProjectionExecutor> expressionExecutors;
  private final List<Accumulator.State> states;

  public AggregateExecutor(
      ExecutorContext context, BatchSchema inputSchema, List<AggregateInfo> aggregateInfos)
      throws ComputeException {
    super(context, inputSchema);
    this.aggregateInfos = new ArrayList<>(aggregateInfos);
    this.expressionExecutors = new ArrayList<>(aggregateInfos.size());
    this.states = new ArrayList<>(aggregateInfos.size());
    try {
      for (AggregateInfo aggregateInfo : aggregateInfos) {
        ProjectionExecutor expressionExecutor =
            new ProjectionExecutor(context, inputSchema, aggregateInfo.getExpressions());
        expressionExecutors.add(expressionExecutor);
        states.add(
            aggregateInfo
                .getAccumulator()
                .initialize(context.getAllocator(), expressionExecutor.getOutputSchema().raw()));
      }
    } catch (ComputeException e) {
      close();
      throw e;
    }
  }

  @Override
  public String getInfo() {
    return "Aggregate" + aggregateInfos;
  }

  @Override
  public boolean needConsume() throws ComputeException {
    for (Accumulator.State state : states) {
      if (state.needMoreData()) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void internalConsume(@WillNotClose Batch batch) throws ComputeException {
    for (int i = 0; i < aggregateInfos.size(); i++) {
      Accumulator.State state = states.get(i);
      if (state.needMoreData()) {
        try (Batch exprResult = expressionExecutors.get(i).compute(batch)) {
          state.accumulate(exprResult.raw());
        }
      }
    }
  }

  private BatchSchema outputSchema;
  private Batch result;

  @Override
  protected void internalFinish() throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    try {
      for (Accumulator.State state : states) {
        FieldVector result = state.evaluate();
        if (result.getMinorType() == Types.MinorType.STRUCT) {
          // unnest multi-column results
          try (NonNullableStructVector structVector = (NonNullableStructVector) result) {
            for (FieldVector fieldVector : structVector.getChildrenFromFields()) {
              results.add(ValueVectors.transfer(context.getAllocator(), fieldVector));
            }
          }
        } else {
          results.add(result);
        }
      }
      if (results.stream().map(FieldVector::getValueCount).anyMatch(count -> count != 1)) {
        throw new ComputeException("Aggregate result should have exactly one row");
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    result = new Batch(new VectorSchemaRoot(results));
    outputSchema = result.getSchema();
  }

  @Override
  public boolean canProduce() {
    return result != null;
  }

  @Override
  public BatchSchema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      throw new ComputeException("Cannot produce batch before finish");
    }
    return outputSchema;
  }

  @Override
  protected Batch internalProduce() {
    Batch out = result;
    result = null;
    return out;
  }

  @Override
  public void close() throws ComputeException {
    List<ComputingCloseable> closeables = new ArrayList<>();
    if (result != null) {
      closeables.add(result::close);
    }
    closeables.addAll(states);
    closeables.addAll(expressionExecutors);
    ComputingCloseables.close(closeables);
  }
}
