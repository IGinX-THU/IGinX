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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.StatefulUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillCloseWhenClosed;
import java.util.List;
import java.util.Objects;

public class UnarySinkMemoryPhysicalTask extends UnaryMemoryPhysicalTask {

  private final UnaryExecutorFactory<? extends StatefulUnaryExecutor> executorFactory;

  public UnarySinkMemoryPhysicalTask(
      PhysicalTask parentTask,
      List<Operator> operators,
      RequestContext context,
      UnaryExecutorFactory<? extends StatefulUnaryExecutor> executorFactory) {
    super(parentTask, operators, context);
    this.executorFactory = Objects.requireNonNull(executorFactory);
  }

  private String info;

  @Override
  public String getInfo() {
    return info;
  }

  @Override
  protected BatchStream compute(BatchStream previous) throws PhysicalException {
    StatefulUnaryExecutor executor = null;
    BatchSchema schema = previous.getSchema();
    BatchSchema outputSchema;
    try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
      executor = executorFactory.initialize(executorContext, schema);
      outputSchema = BatchSchema.of(executor.getOutputSchema());
      info = executor.toString();
    } catch (ComputeException e) {
      try (BatchStream previousHolder = previous;
           StatefulUnaryExecutor executorHolder = executor) {
        throw e;
      }
    }
    while (true) {
      try (Batch batch = previous.getNext()) {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          if (!executor.consume(batch.raw())) {
            break;
          }
        }
      }
    }
    return new UnarySinkBatchStream(previous, outputSchema, executor);
  }

  private class UnarySinkBatchStream implements BatchStream {

    private final BatchStream source;
    private final BatchSchema outputSchema;
    private final StatefulUnaryExecutor executor;

    public UnarySinkBatchStream(@WillCloseWhenClosed BatchStream source, BatchSchema outputSchema, @WillCloseWhenClosed StatefulUnaryExecutor executor) {
      this.source = Objects.requireNonNull(source);
      this.outputSchema = Objects.requireNonNull(outputSchema);
      this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public BatchSchema getSchema() throws ComputeException {
      return outputSchema;
    }

    @Override
    public Batch getNext() throws PhysicalException {
      while (true) {
        try (Batch batch = source.getNext()) {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            if (!executor.consume(batch.raw())) {
              break;
            }
          }
        }
      }
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        VectorSchemaRoot compute = executor.produce();
        getMetrics().accumulateAffectRows(compute.getRowCount());
        return Batch.of(compute);
      }
    }

    @Override
    public void close() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        try (BatchStream source = this.source;
             StatefulUnaryExecutor executor = this.executor) {
          // Do nothing
        }
      }
    }
  }
}
