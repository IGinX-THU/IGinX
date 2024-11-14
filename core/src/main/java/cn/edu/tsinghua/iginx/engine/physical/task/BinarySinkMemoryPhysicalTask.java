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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public class BinarySinkMemoryPhysicalTask extends BinaryMemoryPhysicalTask {

  private final BinaryExecutorFactory<StatefulBinaryExecutor> executorFactory;

  public BinarySinkMemoryPhysicalTask(
      PhysicalTask parentTaskA,
      PhysicalTask parentTaskB,
      List<Operator> operators,
      RequestContext context,
      BinaryExecutorFactory<StatefulBinaryExecutor> executorFactory) {
    super(operators, parentTaskA, parentTaskB, context);
    this.executorFactory = executorFactory;
  }

  private String info;

  @Override
  public String getInfo() {
    return info == null ? super.getInfo() : info;
  }

  @Override
  protected BatchStream compute(BatchStream left, BatchStream right) throws PhysicalException {
    StatefulBinaryExecutor executor = null;
    try {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        executor = executorFactory.initialize(executorContext, left.getSchema(), right.getSchema());
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
    public Batch getNext() throws PhysicalException {
      fetchAndConsume(executor, leftSource, rightSource);
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
        try (Batch leftBatch = leftSource.getNext()) {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            executor.consumeLeft(leftBatch);
          }
        }
      }
      if (executor.needConsumeRight()) {
        try (Batch rightBatch = rightSource.getNext()) {
          try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
            executor.consumeRight(rightBatch);
          }
        }
      }
    }
  }
}
