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
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.WillCloseWhenClosed;

class FetchAsyncBatchStream implements BatchStream {

  private final BatchStream stream;

  private final Queue<Batch> queue = new ArrayDeque<>();
  private final AtomicBoolean finished = new AtomicBoolean(false);

  public FetchAsyncBatchStream(@WillCloseWhenClosed BatchStream stream) {
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
      return queue.remove();
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
      this.stream.close();
    }
  }

  public synchronized void fetchAll(TaskMetrics metrics) throws PhysicalException {
    try {
      while (!finished.get() && stream.hasNext()) {
        Batch batch = stream.getNext();
        synchronized (queue) {
          metrics.accumulateAffectRows(batch.getRowCount());
          queue.add(batch);
          queue.notify();
        }
      }
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
