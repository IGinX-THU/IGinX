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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.StatelessUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;

@Immutable
public class PipelineMemoryPhysicalTask extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

  private final UnaryExecutorFactory<? extends StatelessUnaryExecutor> executorFactory;

  public PipelineMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask,
      List<Operator> operators,
      RequestContext context,
      UnaryExecutorFactory<? extends StatelessUnaryExecutor> executorFactory) {
    super(parentTask, operators, context);
    this.executorFactory = Objects.requireNonNull(executorFactory);
  }

  private String info;

  public UnaryExecutorFactory<? extends StatelessUnaryExecutor> getExecutorFactory() {
    return executorFactory;
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public String getInfo() {
    return info == null ? super.getInfo() : info;
  }

  @Override
  protected BatchStream compute(@WillClose BatchStream previous) throws PhysicalException {
    StatelessUnaryExecutor executor = null;
    BatchSchema outputSchema;
    try {
      BatchSchema schema = previous.getSchema();
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        executor = executorFactory.initialize(executorContext, schema);
        outputSchema = BatchSchema.of(executor.getOutputSchema());
        info = executor.toString();
      }
    } catch (PhysicalException e) {
      try (BatchStream previousHolder = previous;
          StatelessUnaryExecutor executorHolder = executor) {
        throw e;
      }
    }
    return new PipelineBatchStream(previous, outputSchema, executor);
  }

  private class PipelineBatchStream implements BatchStream {

    private final BatchStream source;
    private final BatchSchema outputSchema;
    private final StatelessUnaryExecutor executor;

    public PipelineBatchStream(
        @WillCloseWhenClosed BatchStream source,
        BatchSchema outputSchema,
        StatelessUnaryExecutor executor) {
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
      return (!executor.isEmpty()) && source.hasNext();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      try (Batch sourceNext = source.getNext()) {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          Batch computed = executor.compute(sourceNext);
          getMetrics().accumulateAffectRows(computed.getRowCount());
          return computed;
        }
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
