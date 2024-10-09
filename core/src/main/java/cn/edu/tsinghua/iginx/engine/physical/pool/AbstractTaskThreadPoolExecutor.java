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
package cn.edu.tsinghua.iginx.engine.physical.pool;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.Map;
import java.util.concurrent.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/** wrapped thread pool for memory/storage tasks */
public class AbstractTaskThreadPoolExecutor extends ThreadPoolExecutor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractTaskThreadPoolExecutor.class);

  private final Map<Thread, PythonInterpreter> threadInterpreterMap = new ConcurrentHashMap<>();

  private static final PythonInterpreterConfig config = FunctionManager.getInstance().getConfig();

  private static class TaskThreadFactory implements ThreadFactory {
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(@NotNull Runnable r) {
      Thread thread = defaultFactory.newThread(r);
      thread.setUncaughtExceptionHandler(
          (t, e) -> {
            // 线程异常退出时，手动清除threadlocal里的interpreter但暂不关闭，留待线程池完全关闭时一起回收
            LOGGER.error("Uncaught exception in thread: " + t.getName(), e);
            ThreadInterpreterManager.cleanupInterpreter();
          });
      return thread;
    }
  }

  /** 创建 fixed thread pool */
  public AbstractTaskThreadPoolExecutor(int poolSize) {
    super(
        poolSize,
        poolSize,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new TaskThreadFactory());
  }

  /** 创建参数自定义的 thread pool */
  public AbstractTaskThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> queue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, queue, new TaskThreadFactory());
  }

  /** 为每个线程创建interpreter，并将其保存到threadlocal中 */
  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    threadInterpreterMap.computeIfAbsent(t, k -> new PythonInterpreter(config));
    ThreadInterpreterManager.setInterpreter(threadInterpreterMap.get(t));
  }

  /** 所有线程已被关闭，线程池退出前关闭所有interpreter资源 */
  @Override
  protected void terminated() {
    super.terminated();
    threadInterpreterMap.values().forEach(PythonInterpreter::close);
    threadInterpreterMap.clear();
  }
}
