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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPhysicalTask implements PhysicalTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPhysicalTask.class);

  private final RequestContext context;
  protected final ExecutorContext executorContext = new PhysicalTaskExecutorContext(this);

  private final TaskType type;

  private final List<Operator> operators;
  private final CountDownLatch resultLatch = new CountDownLatch(1);
  private PhysicalTask followerTask;
  private TaskResult result;

  private final TaskMetrics metrics = new TaskMetrics();

  public AbstractPhysicalTask(TaskType type, List<Operator> operators, RequestContext context) {
    this.type = type;
    this.operators = operators;
    this.context = context;
  }

  @Override
  public RequestContext getContext() {
    return context;
  }

  @Override
  public TaskType getType() {
    return type;
  }

  @Override
  public List<Operator> getOperators() {
    return operators;
  }

  @Override
  public PhysicalTask getFollowerTask() {
    return followerTask;
  }

  @Override
  public void setFollowerTask(PhysicalTask task) {
    this.followerTask = task;
  }

  public TaskMetrics getMetrics() {
    return metrics;
  }

  @Override
  public TaskResult getResult() {
    try {
      this.resultLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("unexpected interrupted when get result: ", e);
    }
    return result;
  }

  @Override
  public void setResult(TaskResult result) {
    Objects.requireNonNull(result);
    this.result = result;
    this.resultLatch.countDown();
  }

  @Override
  public String getInfo() {
    List<String> info =
        operators.stream()
            .map(op -> op.getType() + ":{" + op.getInfo() + "}")
            .collect(Collectors.toList());
    return String.join(",", info);
  }

  @Deprecated
  public void setResult(TaskExecuteResult result) {
    if (result == null) {
      setResult(new TaskResult());
    } else if (result.getException() != null) {
      setResult(new TaskResult(result.getException()));
    } else {
      RowStream rowStream = result.getRowStream();
      if (rowStream != null) {
        BatchStream batchStream =
            BatchStreams.wrap(
                executorContext.getAllocator(), rowStream, executorContext.getBatchRowCount());
        setResult(new TaskResult(new MetricStream(batchStream)));
      } else {
        setResult(new TaskResult());
      }
    }
  }

  private class MetricStream implements BatchStream {

    private final BatchStream source;

    public MetricStream(BatchStream source) {
      this.source = Objects.requireNonNull(source);
    }

    @Override
    public BatchSchema getSchema() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        return source.getSchema();
      }
    }

    @Override
    public Batch getNext() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        Batch batch = source.getNext();
        getMetrics().accumulateAffectRows(batch.getRowCount());
        return batch;
      }
    }

    @Override
    public void close() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        source.close();
      }
    }
  }
}
