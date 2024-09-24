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

import cn.edu.tsinghua.iginx.engine.shared.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import cn.edu.tsinghua.iginx.utils.Pair;
import pemja.core.PythonInterpreter;

public abstract class PyUDF implements Function {

  protected final BlockingQueue<PythonInterpreter> interpreters;

  protected final String moduleName;

  public PyUDF(BlockingQueue<PythonInterpreter> interpreters, String moduleName) {
    this.interpreters = interpreters;
    this.moduleName = moduleName;
  }

  public void close() {
    while (!interpreters.isEmpty()) {
      PythonInterpreter interpreter = interpreters.poll();
      if (interpreter != null) {
        // remove the module
        interpreter.exec(String.format("import sys; sys.modules.pop('%s', None)", moduleName));
        interpreter.close();
      }
    }
  }


  /**
   * 将位置参数转为一个2*n的列表传给python udf，包含列名和位置参数，其在列表中的位置就是它在调用udf时所在的位置
   *
   * @param posArgs 位置参数，包含列名和位置参数
   * @return 二维列表，为了传给python
   */
  public static final List<List<Object>> getPyPosParams(List<Pair<Integer, Object>> posArgs) {
    List<List<Object>> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      list.add(new ArrayList<>(Arrays.asList(p.k, p.v)));
    }
    return list;
  }
}
