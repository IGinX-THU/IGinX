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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import java.util.Collections;
import java.util.NoSuchElementException;

public class BinarySinkMemoryPhysicalTask
    extends BinaryMemoryPhysicalTask<BatchStream, BatchStream> {

  private final BinaryExecutorFactory<StatefulBinaryExecutor> executorFactory;

  public BinarySinkMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTaskA,
      PhysicalTask<BatchStream> parentTaskB,
      BinaryOperator operator,
      RequestContext context,
      BinaryExecutorFactory<StatefulBinaryExecutor> executorFactory) {
    super(Collections.singletonList(operator), parentTaskA, parentTaskB, context);
    this.executorFactory = executorFactory;
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
  protected BatchStream compute(BatchStream left, BatchStream right) throws PhysicalException {
    StatefulBinaryExecutor executor = null;
    try {
      BatchSchema leftSchema = left.getSchema();
      BatchSchema rightSchema = right.getSchema();
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        executor = executorFactory.initialize(executorContext, leftSchema, rightSchema);
      }
      info = executor.toString();
      fetchAndConsume(executor, left, right);
    } catch (PhysicalException e) {
      try (StatefulBinaryExecutor executorHolder = executor;
          BatchStream leftHolder = left;
          BatchStream rightHolder = right) {
        throw e;
      }
    }
    return new BinarySinkBatchStream(left, right, executor);
  }

  private class BinarySinkBatchStream implements BatchStream {

    private final BatchStream leftSource;
    private final BatchStream rightSource;
    private final StatefulBinaryExecutor executor;

    public BinarySinkBatchStream(
        BatchStream leftSource, BatchStream rightSource, StatefulBinaryExecutor executor) {
      this.leftSource = leftSource;
      this.rightSource = rightSource;
      this.executor = executor;
    }

    private BatchSchema schema;

    @Override
    public BatchSchema getSchema() throws ComputeException {
      if (schema == null) {
        schema = BatchSchema.of(executor.getOutputSchema());
      }
      return schema;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      if (executor.canProduce()) {
        return true;
      }
      fetchAndConsume(executor, leftSource, rightSource);
      return executor.canProduce();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        Batch produced = executor.produce();
        getMetrics().accumulateAffectRows(produced.getRowCount());
        return produced;
      }
    }

    @Override
    public void close() throws PhysicalException {
      try (BatchStream leftSource = this.leftSource;
          BatchStream rightSource = this.rightSource) {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          executor.close();
        }
      }
    }
  }

  private void fetchAndConsume(
      StatefulBinaryExecutor executor, BatchStream leftSource, BatchStream rightSource)
      throws PhysicalException {
    while (executor.needConsumeLeft() || executor.needConsumeRight()) {
      if (executor.needConsumeLeft()) {
        if (leftSource.hasNext()) {
          try (Batch leftBatch = leftSource.getNext()) {
            try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
              executor.consumeLeft(leftBatch);
            }
          }
        } else {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            executor.consumeLeftEnd();
          }
        }
      }
      if (executor.needConsumeRight()) {
        if (rightSource.hasNext()) {
          try (Batch rightBatch = rightSource.getNext()) {
            try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
              executor.consumeRight(rightBatch);
            }
          }
        } else {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            executor.consumeRightEnd();
          }
        }
      }
    }
  }
}
