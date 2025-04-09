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
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.validation.constraints.NotNull;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/**
 * 管理线程上的interpreter，存到ThreadLocal中以便随时随地取用
 *
 * <p>不管理interpreter的生命周期，生命周期管理在AbstractTaskThreadPoolExecutor中
 */
public class ThreadInterpreterManager {
  private static final ThreadLocal<PythonInterpreter> interpreterThreadLocal = new ThreadLocal<>();
  private static final ThreadLocal<PythonInterpreterConfig> configThreadLocal = new ThreadLocal<>();

  @NotNull
  public static PythonInterpreter getInterpreter() {
    PythonInterpreter interpreter = interpreterThreadLocal.get();
    if (interpreter == null) {
      setInterpreter(new PythonInterpreter(configThreadLocal.get()));
    }
    return interpreterThreadLocal.get();
  }

  public static boolean isInterpreterSet() {
    return interpreterThreadLocal.get() != null;
  }

  public static void setInterpreter(@NotNull PythonInterpreter interpreter) {
    interpreterThreadLocal.set(interpreter);
  }

  public static void setConfig(@NotNull PythonInterpreterConfig config) {
    configThreadLocal.set(config);
  }

  public static void executeWithInterpreter(Consumer<PythonInterpreter> action) {
    PythonInterpreter interpreter = getInterpreter();
    action.accept(interpreter);
  }

  public static <T> T executeWithInterpreterAndReturn(Function<PythonInterpreter, T> action) {
    PythonInterpreter interpreter = getInterpreter();
    return action.apply(interpreter);
  }

  @SuppressWarnings("unchecked")
  public static <T> T invokeMethodWithTimeout(
      long timeout,
      String obj,
      String function,
      List<List<Object>> data,
      List<Object> args,
      Map<String, Object> kvargs) {
    if (timeout <= 0) {
      return invokeMethod(obj, function, data, args, kvargs);
    }
    return (T)
        executeWithInterpreterAndReturn(
            interpreter -> {
              // Wrap the original object with TimeoutSafeWrapper.
              // All function calls on the wrapped instance will be automatically
              // managed with a timeout.
              interpreter.exec("from timeout_handler import TimeoutSafeWrapper");
              String safeObject = String.format("%s_safe_instance", obj);
              interpreter.exec(
                  String.format("%s=TimeoutSafeWrapper(%s, %d)", safeObject, obj, timeout));
              return interpreter.invokeMethod(safeObject, function, data, args, kvargs);
            });
  }

  @SuppressWarnings("unchecked")
  public static <T> T invokeMethod(
      String obj,
      String function,
      List<List<Object>> data,
      List<Object> args,
      Map<String, Object> kvargs) {
    return (T)
        executeWithInterpreterAndReturn(
            interpreter -> interpreter.invokeMethod(obj, function, data, args, kvargs));
  }
}
