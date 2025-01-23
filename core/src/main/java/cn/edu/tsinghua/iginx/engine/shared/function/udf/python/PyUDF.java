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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public abstract class PyUDF implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(PyUDF.class);

  protected final String moduleName;

  protected final String className;

  public PyUDF(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }

  public void close(String funcName, PythonInterpreter interpreter) {
    try {
      interpreter.exec(String.format("import sys; sys.modules.pop('%s', None)", moduleName));
    } catch (NullPointerException e) {
      LOGGER.error("Did not find module {} for function {}", moduleName, funcName);
    } catch (Exception e) {
      LOGGER.error("Remove module for udf {} failed:", funcName, e);
    }
  }

  protected List<List<Object>> invokePyUDF(
      List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
    try {
      // 由于多个UDF共享interpreter，因此使用独特的对象名
      String obj = (moduleName + className).replace(".", "a");
      ThreadInterpreterManager.executeWithInterpreter(
          interpreter ->
              interpreter.exec(
                  String.format(
                      "import %s; %s = %s.%s()", moduleName, obj, moduleName, className)));
      return (List<List<Object>>)
          ThreadInterpreterManager.executeWithInterpreterAndReturn(
              interpreter -> interpreter.invokeMethod(obj, UDF_FUNC, data, args, kvargs));
    } catch (Exception e) {
      LOGGER.error("Invoke python failure: ", e);
      return null;
    }
  }
}
