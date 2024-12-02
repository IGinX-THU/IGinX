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
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
class GatherBatchStream implements BatchStream {

  private final TaskMetrics metrics;
  private final ScatterBatchStream scatterStream;
  private final PriorityQueue<Batch> queue =
      new PriorityQueue<>(Comparator.comparingLong(Batch::getSequenceNumber));
  private long nextSequenceNumber = 0;
  private boolean finished = false;
  private Batch cached;
  private volatile BatchSchema schema;

  public GatherBatchStream(
      @WillCloseWhenClosed ScatterBatchStream scatterStream, TaskMetrics metrics) {
    this.scatterStream = Objects.requireNonNull(scatterStream);
    this.metrics = Objects.requireNonNull(metrics);
  }

  @Override
  public synchronized BatchSchema getSchema() throws PhysicalException {
    while (schema == null) {
      if (finished) {
        throw new PhysicalException("GatherBatchStream is finished without giving schema");
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new PhysicalException(e);
      }
    }
    return schema;
  }

  @Override
  public synchronized boolean hasNext() throws PhysicalException {
    if (cached == null) {
      cached = fetch();
      return cached != null;
    } else {
      return true;
    }
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Batch batch = cached;
    metrics.accumulateAffectRows(batch.getRowCount());
    cached = null;
    return batch;
  }

  private synchronized Batch fetch() throws PhysicalException {
    while (!finished) {
      Batch head = queue.peek();
      if (head != null) {
        if (head.getSequenceNumber() == nextSequenceNumber) {
          nextSequenceNumber++;
          return queue.poll();
        }
      }
      if (scatterStream.finished.get()
          && nextSequenceNumber == scatterStream.nextSequenceNumber.get()) {
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

  @Override
  public synchronized void close() throws PhysicalException {
    finished = true;
    scatterStream.close();
    queue.forEach(Batch::close);
    queue.clear();
    if (cached != null) {
      cached.close();
      cached = null;
    }
  }

  public void offer(BatchStream previous, TaskMetrics metrics) throws PhysicalException {
    try {
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
    } catch (Exception e) {
      synchronized (this) {
        finished = true;
      }
      throw e;
    } finally {
      synchronized (this) {
        notifyAll();
      }
    }
  }
}
