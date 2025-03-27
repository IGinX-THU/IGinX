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
package cn.edu.tsinghua.iginx.engine.physical.task.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TaskResultMap implements PhysicalCloseable {

  private final HashMap<PhysicalTask<?>, Entry> results = new HashMap<>();
  private volatile boolean closed = false;

  @Override
  public synchronized void close() throws PhysicalException {
    if (closed) {
      return;
    }
    closed = true;
    PhysicalCloseables.close(results.values());
  }

  public synchronized void put(PhysicalTask<?> task, TaskResult<?> result) {
    if (closed) {
      try {
        result.close();
      } catch (PhysicalException e) {
        throw new IllegalStateException("Failed to close result", e);
      }
    }
    if (!results.containsKey(task)) {
      Entry entry = new Entry<>();
      results.put(task, entry);
    }
    results.get(task).setOnce(result);
  }

  public synchronized <RESULT extends PhysicalCloseable> Future<TaskResult<RESULT>> get(
      PhysicalTask<RESULT> task) {
    if (closed) {
      throw new IllegalStateException("TaskResultMap is closed");
    }
    if (results.containsKey(task)) {
      return results.get(task);
    }
    Entry<RESULT> entry = new Entry<>();
    results.put(task, entry);
    return entry;
  }

  @ThreadSafe
  private static class Entry<RESULT extends PhysicalCloseable>
      implements PhysicalCloseable, Future<TaskResult<RESULT>> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile TaskResult result = null;

    public synchronized void setOnce(TaskResult<?> result) {
      if (this.result != null || latch.getCount() == 0) {
        throw new IllegalStateException("Result is already set");
      }
      this.result = result;
      latch.countDown();
    }

    private synchronized TaskResult<RESULT> getOnce() {
      if (result == null) {
        throw new IllegalStateException("Result is not set");
      }
      TaskResult result = this.result;
      this.result = null;
      return result;
    }

    @Override
    public synchronized void close() throws PhysicalException {
      if (result != null) {
        result.close();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return latch.getCount() == 0;
    }

    @Override
    public TaskResult<RESULT> get() throws InterruptedException {
      latch.await();
      return getOnce();
    }

    @Override
    public TaskResult<RESULT> get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      if (!latch.await(timeout, unit)) {
        throw new TimeoutException();
      }
      return getOnce();
    }
  }
}
