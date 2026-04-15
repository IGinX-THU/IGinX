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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableQueue;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.*;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class Compactor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);

  private final String name;
  private final Semaphore flusherPermits;
  private final Duration memTableTimeout;
  private final MemTableQueue memTableQueue;
  private final TableStorage tableStorage;
  private volatile long idBase;

  private ExecutorService flusher = null;
  private ExecutorService flushDispatcher = null;
  private ExecutorService deleter = null;
  private ScheduledExecutorService scheduler = null;

  public Compactor(
      String name,
      Semaphore flusherPermits,
      Duration memTableTimeout,
      MemTableQueue memTableQueue,
      TableStorage tableStorage) {
    this.name = name;
    this.flusherPermits = flusherPermits;
    this.memTableTimeout = memTableTimeout;
    this.memTableQueue = memTableQueue;
    this.tableStorage = tableStorage;
  }

  public void start() {
    idBase = System.nanoTime();
    Preconditions.checkState(flusher == null);
    ThreadFactory workerFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("flusher-" + name + "-worker-%d")
            .setUncaughtExceptionHandler(this::handleUncaughtException)
            .build();
    flusher = Executors.newCachedThreadPool(workerFactory);

    Preconditions.checkState(flushDispatcher == null);
    ThreadFactory dispatcherFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("flusher-" + name + "-dispatcher-%d")
            .setUncaughtExceptionHandler(this::handleUncaughtException)
            .build();
    flushDispatcher = Executors.newSingleThreadExecutor(dispatcherFactory);
    flushDispatcher.submit(this::flushLoop);

    Preconditions.checkState(deleter == null);
    ThreadFactory deleterFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("flusher-" + name + "-deleter-%d")
            .setUncaughtExceptionHandler(this::handleUncaughtException)
            .build();
    deleter = Executors.newCachedThreadPool(deleterFactory);
    deleter.submit(this::deleteLoop);

    Preconditions.checkState(scheduler == null);
    ThreadFactory schedulerFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("flusher-" + name + "-scheduler-%d")
            .setUncaughtExceptionHandler(this::handleUncaughtException)
            .build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(schedulerFactory);
    if (!memTableTimeout.isZero() && !memTableTimeout.isNegative()) {
      LOGGER.info("flusher {} start to force flush every {}", name, memTableTimeout);
      long ns = memTableTimeout.toNanos();
      this.scheduler.scheduleWithFixedDelay(this::schedule, ns, ns, TimeUnit.NANOSECONDS);
    }
  }

  public void stop() throws InterruptedException {
    scheduler.shutdownNow();
    awaitTermination(scheduler);
    scheduler = null;

    flushDispatcher.shutdownNow();
    awaitTermination(flushDispatcher);
    flushDispatcher = null;

    flusher.shutdown();
    awaitTermination(flusher);
    flusher = null;

    deleter.shutdownNow();
    awaitTermination(deleter);
    deleter = null;
  }

  private void awaitTermination(ExecutorService executor) throws InterruptedException {
    while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
      LOGGER.warn("executor {} did not terminate in time, continue waiting", executor);
    }
  }

  private void flushLoop() {
    while (!Thread.currentThread().isInterrupted()) {
      MemTableQueue.TookTable table = null;
      try {
        table = memTableQueue.takeToFlush();
        flusherPermits.acquire();
      } catch (InterruptedException ignored) {
        if (table != null) {
          table.close();
        }
        break;
      }
      long tableId = idBase + table.getId();
      MemTableQueue.TookTable finalTable = table;
      flusher.submit(
          () -> {
            try (MemTableQueue.TookTable ignored = finalTable) {
              LOGGER.debug("start to flush table {}", tableId);
              tableStorage.flush(tableId, finalTable.getMemTable(), finalTable.isFinalTable());
            } catch (Throwable e) {
              finalTable.fail();
              LOGGER.error("flush memtable {} failed", tableId, e);
            } finally {
              flusherPermits.release();
              LOGGER.debug("end to flush table {}", tableId);
            }
          });
      LOGGER.debug("memtable {} is submit to flush", tableId);
    }
    LOGGER.info("flusher {} dispatch loop is exited", name);
  }

  private void deleteLoop() {
    while (!Thread.currentThread().isInterrupted()) {
      long id;
      try {
        id = memTableQueue.takeToDelete();
      } catch (InterruptedException ignored) {
        break;
      }
      long tableId = idBase + id;
      LOGGER.debug("flushed table {} is need to delete", tableId);
      try {
        tableStorage.deleteUncommitted(tableId);
      } catch (Throwable e) {
        LOGGER.error("delete memtable {} failed, giving up", tableId, e);
      }
    }
    LOGGER.info("flusher {} delete loop is exited", name);
  }

  private void schedule() {
    try {
      memTableQueue.flushAll(true);
    } catch (InterruptedException ignored) {
      LOGGER.info("flusher {} schedule task is cancel", name);
    }
  }

  private void handleUncaughtException(Thread t, Throwable e) {
    LOGGER.error("flusher {} thread {} is error", name, t, e);
  }
}
