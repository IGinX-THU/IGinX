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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;

public class FetchAsyncMemoryPhysicalTask
    extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

  public FetchAsyncMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask, RequestContext context) {
    super(parentTask, Collections.emptyList(), context);
  }

  @Override
  protected BatchStream compute(@WillClose BatchStream previous) {
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      ExchangeBatchStream stream = new ExchangeBatchStream(previous);
      if (getFollowerTask() != null) {
        MemoryPhysicalTask<?> followerTask = (MemoryPhysicalTask<?>) getFollowerTask();
        if (followerTask.notifyParentReady()) {
          getContext().getPhysicalEngine().submit(followerTask);
        }
      }
      setFollowerTask(new BatchFetcher(stream));
      return stream;
    }
  }

  @Override
  public String getInfo() {
    return "FetchAsync";
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public Future<TaskResult<BatchStream>> getResult() {
    return super.getResult();
  }

  @Override
  public void setResult(TaskResult<?> result) {
    super.setResult(result);
  }

  private class BatchFetcher extends MemoryPhysicalTask<BatchStream> {

    private final ExchangeBatchStream exchangeStream;

    public BatchFetcher(ExchangeBatchStream exchangeStream) {
      super(
          TaskType.UnaryMemory,
          Collections.emptyList(),
          FetchAsyncMemoryPhysicalTask.this.getContext(),
          1);
      this.exchangeStream = Objects.requireNonNull(exchangeStream);
    }

    @Override
    public void accept(TaskVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getInfo() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<BatchStream> getResultClass() {
      return BatchStream.class;
    }

    @Override
    public TaskResult<BatchStream> execute() {
      exchangeStream.fetchAll();
      return new TaskResult<>(BatchStreams.empty());
    }
  }

  private class ExchangeBatchStream implements BatchStream {

    private final BatchStream stream;

    private final Queue<Batch> queue = new ArrayDeque<>();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private PhysicalException exception;

    public ExchangeBatchStream(@WillCloseWhenClosed BatchStream stream) {
      this.stream = Objects.requireNonNull(stream);
    }

    @Override
    public BatchSchema getSchema() throws PhysicalException {
      return stream.getSchema();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      try {
        synchronized (queue) {
          while (queue.isEmpty()) {
            if (finished.get()) {
              return false;
            }
            queue.wait();
          }
          return true;
        }
      } catch (InterruptedException e) {
        throw new PhysicalException(e);
      }
    }

    @Override
    public Batch getNext() throws PhysicalException {
      synchronized (queue) {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Batch batch = queue.remove();
        FetchAsyncMemoryPhysicalTask.this.getMetrics().accumulateAffectRows(batch.getRowCount());
        return batch;
      }
    }

    @Override
    public void close() throws PhysicalException {
      setFinished();
      synchronized (this) {
        synchronized (queue) {
          queue.forEach(Batch::close);
          queue.clear();
        }
        try (BatchStream ignored = this.stream) {
          if (exception != null) {
            throw exception;
          }
        }
      }
    }

    public synchronized void fetchAll() {
      try {
        while (!finished.get() && stream.hasNext()) {
          Batch batch = stream.getNext();
          synchronized (queue) {
            queue.add(batch);
            queue.notify();
          }
        }
      } catch (PhysicalException e) {
        exception = e;
      } finally {
        setFinished();
      }
    }

    private void setFinished() {
      finished.set(true);
      synchronized (queue) {
        queue.notifyAll();
      }
    }
  }
}
