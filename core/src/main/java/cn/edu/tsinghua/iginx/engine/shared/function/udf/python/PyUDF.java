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
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PyUDF implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(PyUDF.class);

  protected final String moduleName;

  protected final String className;

  public PyUDF(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }

  public void close(String funcName) {
    try {
      ThreadInterpreterManager.executeWithInterpreter(
          interpreter ->
              interpreter.exec(
                  String.format("import sys; sys.modules.pop('%s', None)", moduleName)));
    } catch (Exception e) {
      LOGGER.error("Remove module for udf {} failed:", funcName, e);
    }
  }

  protected List<List<Object>> invokePyUDF(
      List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
    try {
      ThreadInterpreterManager.executeWithInterpreter(
          interpreter ->
              interpreter.exec(
                  String.format("import %s; t = %s.%s()", moduleName, moduleName, className)));
      return (List<List<Object>>)
          ThreadInterpreterManager.executeWithInterpreterAndReturn(
              interpreter -> interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs));
    } catch (Exception e) {
      LOGGER.error("Invoke python failure: ", e);
      return null;
    }
  }
}
