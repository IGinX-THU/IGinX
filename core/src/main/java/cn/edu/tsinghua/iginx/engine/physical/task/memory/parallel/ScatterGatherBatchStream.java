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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskMetrics;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ScatterGatherBatchStream extends PrefetchBatchStream {

  private final ScatterBatchStream scatterStream;
  private final TaskMetrics metrics;
  private final CountDownLatch unfinished;
  private final List<BatchStream> branches;
  private boolean interrupted = false;

  private BatchSchema schema;
  private final PriorityQueue<Batch> queue =
      new PriorityQueue<>(Comparator.comparingLong(Batch::getSequenceNumber));
  private long nextSequenceNumber = 0;

  public ScatterGatherBatchStream(
      @WillCloseWhenClosed BatchStream previous, TaskMetrics metrics, int branch) {
    this.scatterStream = new ScatterBatchStream(previous);
    this.metrics = Objects.requireNonNull(metrics);
    this.unfinished = new CountDownLatch(branch);

    List<BatchStream> branched = new ArrayList<>(branch);
    for (int i = 0; i < branch; i++) {
      branched.add(new ScatterBranchBatchStream(scatterStream, unfinished));
    }
    this.branches = Collections.unmodifiableList(branched);
  }

  public List<BatchStream> getBranches() {
    return branches;
  }

  @Override
  public void close() throws PhysicalException {
    try {
      unfinished.await();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    synchronized (this) {
      super.close();
      interrupted = true;
      queue.forEach(Batch::close);
      queue.clear();
      scatterStream.close();
    }
  }

  @Override
  public synchronized BatchSchema getSchema() throws PhysicalException {
    while (schema == null) {
      if (interrupted) {
        throw new IllegalStateException("GatherBatchStream is interrupted");
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    return schema;
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    Batch batch = super.getNext();
    metrics.accumulateAffectRows(batch.getRowCount());
    return batch;
  }

  @Nullable
  @Override
  protected synchronized Batch prefetchBatch() throws PhysicalException {
    while (!interrupted) {
      Batch head = queue.peek();
      if (head != null) {
        if (head.getSequenceNumber() == nextSequenceNumber) {
          nextSequenceNumber++;
          return queue.poll();
        }
      }
      if (!scatterStream.hasNext() && nextSequenceNumber == scatterStream.getNextSequenceNumber()) {
        break;
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new PhysicalException(e);
      }
    }
    return null;
  }

  public void offer(TaskResult<BatchStream> result, TaskMetrics metrics) throws PhysicalException {
    try (BatchStream previous = result.unwrap()) {
      synchronized (this) {
        if (this.schema == null) {
          this.schema = previous.getSchema();
          notifyAll();
        }
      }
      while (previous.hasNext()) {
        Batch batch = previous.getNext();
        metrics.accumulateAffectRows(batch.getRowCount());
        synchronized (this) {
          queue.offer(batch);
          Batch head = queue.peek();
          if (head.getSequenceNumber() == nextSequenceNumber) {
            notifyAll();
          }
        }
      }
    } catch (Throwable e) {
      synchronized (this) {
        interrupted = true;
        notifyAll();
      }
      throw e;
    }
  }

  private static class ScatterBranchBatchStream extends PrefetchBatchStream {
    private final ScatterBatchStream previous;
    private final CountDownLatch onClose;

    public ScatterBranchBatchStream(
        @WillNotClose ScatterBatchStream previous, CountDownLatch onClose) {
      this.previous = previous;
      this.onClose = onClose;
    }

    @Override
    public BatchSchema getSchema() throws PhysicalException {
      return previous.getSchema();
    }

    @Override
    protected Batch prefetchBatch() throws PhysicalException {
      if (!previous.hasNext()) {
        return null;
      }
      try {
        return previous.getNext();
      } catch (NoSuchElementException e) {
        return null;
      }
    }

    @Override
    public void close() {
      onClose.countDown();
    }
  }
}
