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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.pool.InterpreterThreadPoolExecutor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public abstract class PyUDF implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(PyUDF.class);

  protected final String moduleName;

  protected final String className;

  // this should be per UDF
  protected InterpreterThreadPoolExecutor executorService;

  public PyUDF(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }

  public void close() {
    shutdownExecutorService();
    try (PythonInterpreter interpreter =
        new PythonInterpreter(FunctionManager.getInstance().getConfig())) {
      // remove the module
      interpreter.exec(String.format("import sys; sys.modules.pop('%s', None)", moduleName));
    }
  }

  private void shutdownExecutorService() {
    if (executorService == null) return;
    executorService.shutdown();
    try {
      // 等待已提交的任务
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      throw new RuntimeException("Error occurred when shutting down UDF thread pool: ", e);
    }
  }

  protected List<List<Object>> invokePyUDF(
      List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
    if (executorService == null) {
      executorService =
          new InterpreterThreadPoolExecutor(
              5, 10, 60, TimeUnit.SECONDS, FunctionManager.getInstance().getConfig());
    }
    Future<List<List<Object>>> futureResult =
        executorService.submit(
            () -> {
              try {
                PythonInterpreter interpreter = executorService.getInterpreterForCurrentThread();
                interpreter.exec(
                    String.format("import %s; t = %s.%s()", moduleName, moduleName, className));
                List<List<Object>> res =
                    (List<List<Object>>)
                        interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs);
                return res;
              } catch (Exception e) {
                LOGGER.error("Error occurred invoking python UDF:", e);
                return null;
              }
            });

    try {
      return futureResult.get();
    } catch (Exception e) {
      LOGGER.error("Error occurred invoking python UDF:", e);
      return null;
    }
  }
}
