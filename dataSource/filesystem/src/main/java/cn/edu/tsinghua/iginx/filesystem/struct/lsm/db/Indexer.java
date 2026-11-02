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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.*;
import java.util.concurrent.*;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indexer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);
  private final String name;
  private final Semaphore permits;
  private ExecutorService worker = null;

  public Indexer(String name, Semaphore permits) {
    this.name = Objects.requireNonNull(name);
    this.permits = permits;
  }

  public synchronized void start() {
    Preconditions.checkState(worker == null, "indexer %s is already started", name);
    ThreadFactory workerFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("indexer-" + name + "-worker-%d")
            .setUncaughtExceptionHandler(this::handleUncaughtException)
            .build();
    worker = Executors.newSingleThreadExecutor(workerFactory);
  }

  public synchronized void stop() throws InterruptedException {
    if (worker != null) {
      worker.shutdownNow();
      while (!worker.awaitTermination(1, TimeUnit.SECONDS)) {
        LOGGER.warn("executor {} did not terminate in time, continue waiting", worker);
      }
      worker = null;
    }
  }

  private void handleUncaughtException(Thread t, Throwable e) {
    LOGGER.error("indexer {} thread {} is error", name, t, e);
  }

  public synchronized void submit(Runnable runnable) {
    Preconditions.checkState(worker != null, "indexer %s is not started", name);
    worker.submit(
        () -> {
          try {
            permits.acquire();
            try {
              runnable.run();
            } finally {
              permits.release();
            }
          } catch (InterruptedException ignored) {
            LOGGER.info(
                "indexer {} worker thread is interrupted while waiting for permits, exit", name);
          }
        });
  }
}
