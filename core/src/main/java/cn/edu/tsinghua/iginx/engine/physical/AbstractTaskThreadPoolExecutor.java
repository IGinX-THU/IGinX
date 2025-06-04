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
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.concurrent.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreterConfig;

/** wrapped thread pool for memory/storage tasks */
public class AbstractTaskThreadPoolExecutor extends ThreadPoolExecutor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractTaskThreadPoolExecutor.class);

  // 默认使用FunctionManager中的config
  protected final PythonInterpreterConfig config;

  private static class TaskThreadFactory implements ThreadFactory {
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(@NotNull Runnable r) {
      Thread thread = defaultFactory.newThread(r);
      thread.setUncaughtExceptionHandler(
          (t, e) -> {
            LOGGER.error("Uncaught exception in thread: {}", t.getName(), e);
          });
      return thread;
    }
  }

  /** 创建固定大小线程池，使用LinkedBlockingQueue */
  public AbstractTaskThreadPoolExecutor(int poolSize) {
    super(
        poolSize,
        poolSize,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new TaskThreadFactory());
    this.config = FunctionManager.getInstance().getConfig();
  }

  /** 创建固定大小线程池，使用LinkedBlockingQueue, 并指定interpreter config */
  public AbstractTaskThreadPoolExecutor(int poolSize, PythonInterpreterConfig config) {
    super(
        poolSize,
        poolSize,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new TaskThreadFactory());
    this.config = config;
  }

  /** 创建参数自定义的线程池 */
  public AbstractTaskThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> queue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, queue, new TaskThreadFactory());
    this.config = FunctionManager.getInstance().getConfig();
  }

  /** 为每个线程创建interpreter config，并将其保存到threadlocal中 */
  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    ThreadInterpreterManager.setConfig(this.config);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
  }

  /** 所有线程已被关闭，线程池退出前关闭所有interpreter资源 */
  @Override
  protected void terminated() {
    super.terminated();
    if (ThreadInterpreterManager.isInterpreterSet()) {
      ThreadInterpreterManager.getInterpreter().close();
    }
  }
}
