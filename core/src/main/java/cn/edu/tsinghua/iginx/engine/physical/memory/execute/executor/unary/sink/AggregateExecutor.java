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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NoExceptionAutoCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

public class AggregateExecutor extends UnarySinkExecutor {

  private final UnaryExecutorInitializer<List<AggregateInfo>> initializer;
  private List<AggregateInfo> aggregateInfos;

  public AggregateExecutor(UnaryExecutorInitializer<List<AggregateInfo>> initializer) {
    this.initializer = initializer;
  }

  public static class AggregateInfo implements NoExceptionAutoCloseable {
    private final Field resultField;
    private final Accumulator accumulator;
    private final int sourceFieldIndex;

    public AggregateInfo(Field resultField, Accumulator accumulator, int sourceFieldIndex) {
      this.resultField = resultField;
      this.accumulator = accumulator;
      this.sourceFieldIndex = sourceFieldIndex;
    }

    public Field getResultField() {
      return resultField;
    }

    public Accumulator getAccumulator() {
      return accumulator;
    }

    public int getSourceFieldIndex() {
      return sourceFieldIndex;
    }

    @Override
    public String toString() {
      return resultField + " <- " + accumulator.getName() + '(' + sourceFieldIndex + ')';
    }

    @Override
    public void close() {
      accumulator.close();
    }
  }

  @Override
  public String getDescription() {
    return AggregateExecutor.class.getSimpleName() + ": " + aggregateInfos;
  }

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws ComputeException {
    aggregateInfos = initializer.initialize(getContext(), inputSchema);
    BatchSchema.Builder builder = BatchSchema.builder();
    for (AggregateInfo aggregateInfo : aggregateInfos) {
      builder.addField(aggregateInfo.getResultField());
    }
    return builder.build();
  }

  @Override
  protected void internalConsume(@WillNotClose Batch batch) throws ComputeException {
    for (AggregateInfo aggregateInfo : aggregateInfos) {
      ValueVector sourceVector = batch.getVector(aggregateInfo.getSourceFieldIndex());
      aggregateInfo.getAccumulator().accumulate(sourceVector);
    }
  }

  private Object[] values;

  @Override
  protected void internalFinish() throws ComputeException {
    values = new Object[aggregateInfos.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = aggregateInfos.get(i).getAccumulator().evaluate();
    }
  }

  @Override
  public boolean canProduce() {
    return values != null;
  }

  @Override
  protected Batch internalProduce() {
    BatchSchema schema = getOutputSchema();
    try (Batch.Builder builder = new Batch.Builder(getContext().getAllocator(), schema, 1)) {
      builder.append(values);
      values = null;
      return builder.build(1);
    }
  }

  @Override
  public void close() throws ComputeException {
    if (aggregateInfos != null) {
      aggregateInfos.forEach(AggregateInfo::close);
    }
  }
}
