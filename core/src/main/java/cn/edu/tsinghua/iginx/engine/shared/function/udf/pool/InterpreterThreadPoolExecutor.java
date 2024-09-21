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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.pool;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class InterpreterThreadPoolExecutor extends ThreadPoolExecutor {
  private final Map<Thread, PythonInterpreter> threadInterpreterMap = new ConcurrentHashMap<>();

  private final PythonInterpreterConfig config;

  public InterpreterThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      PythonInterpreterConfig config) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>());
    this.config = config;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    threadInterpreterMap.computeIfAbsent(t, k -> new PythonInterpreter(config));
  }

  @Override
  protected void terminated() {
    super.terminated();
    threadInterpreterMap.values().forEach(PythonInterpreter::close);
    threadInterpreterMap.clear();
  }

  public PythonInterpreter getInterpreterForCurrentThread() {
    return threadInterpreterMap.get(Thread.currentThread());
  }
}
