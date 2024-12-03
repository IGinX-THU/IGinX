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
package cn.edu.tsinghua.iginx.vectordb.tools;

import java.util.concurrent.*;

public class TaskExecutor {

  private static ExecutorService executorService;

  static {
    int corePoolSize = 10;
    int maximumPoolSize = 50;
    long keepAliveTime = 60L;
    TimeUnit unit = TimeUnit.SECONDS;
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(100);
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
    RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();

    executorService =
        new ThreadPoolExecutor(
            corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  public static void execute(Runnable runnable) {
    executorService.execute(runnable);
  }

  public static ExecutorService getExecutorService() {
    return executorService;
  }
}
