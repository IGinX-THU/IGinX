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
package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.StatefulUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;

public class UnarySinkMemoryPhysicalTask extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

  private final UnaryExecutorFactory<? extends StatefulUnaryExecutor> executorFactory;

  public UnarySinkMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask,
      List<Operator> operators,
      RequestContext context,
      UnaryExecutorFactory<? extends StatefulUnaryExecutor> executorFactory) {
    super(parentTask, operators, context);
    this.executorFactory = Objects.requireNonNull(executorFactory);
  }

  private String info;

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public String getInfo() {
    return info == null ? super.getInfo() : info;
  }

  @Override
  protected BatchStream compute(BatchStream previous) throws PhysicalException {
    StatefulUnaryExecutor executor = null;
    BatchSchema outputSchema;
    try {
      BatchSchema schema = previous.getSchema();
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        executor = executorFactory.initialize(executorContext, schema);
        info = executor.toString();
      }
      fetchAndConsume(executor, previous);
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        outputSchema = BatchSchema.of(executor.getOutputSchema());
      }
    } catch (ComputeException e) {
      try (BatchStream previousHolder = previous;
          StatefulUnaryExecutor executorHolder = executor) {
        throw e;
      }
    }
    return new UnarySinkBatchStream(previous, outputSchema, executor);
  }

  private void fetchAndConsume(StatefulUnaryExecutor executor, BatchStream source)
      throws PhysicalException {
    while (executor.needConsume()) {
      if (source.hasNext()) {
        try (Batch batch = source.getNext()) {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            executor.consume(batch);
          }
        }
      } else {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          executor.consumeEnd();
        }
      }
    }
  }

  private class UnarySinkBatchStream implements BatchStream {

    private final BatchStream source;
    private final BatchSchema outputSchema;
    private final StatefulUnaryExecutor executor;

    public UnarySinkBatchStream(
        @WillCloseWhenClosed BatchStream source,
        BatchSchema outputSchema,
        @WillCloseWhenClosed StatefulUnaryExecutor executor) {
      this.source = Objects.requireNonNull(source);
      this.outputSchema = Objects.requireNonNull(outputSchema);
      this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public BatchSchema getSchema() throws ComputeException {
      return outputSchema;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      if (!executor.canProduce()) {
        fetchAndConsume(executor, source);
      }
      return executor.canProduce();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        Batch produced = executor.produce();
        getMetrics().accumulateAffectRows(produced.getRowCount());
        return produced;
      }
    }

    @Override
    public void close() throws PhysicalException {
      try (BatchStream source = this.source) {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          executor.close();
        }
      }
    }
  }
}
