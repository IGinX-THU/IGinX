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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import pemja.core.PythonInterpreter;

public class PyUDSF implements UDSF {

  private static final String PY_UDSF = "py_udsf";

  private final BlockingQueue<PythonInterpreter> interpreters;

  private final String funcName;

  public PyUDSF(BlockingQueue<PythonInterpreter> interpreters, String funcName) {
    this.interpreters = interpreters;
    this.funcName = funcName;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.UDF;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.Mapping;
  }

  @Override
  public String getIdentifier() {
    return PY_UDSF;
  }

  @Override
  public RowStream transform(Table table, FunctionParams params) throws Exception {
    if (!CheckUtils.isLegal(params)) {
      throw new IllegalArgumentException("unexpected params for PyUDSF.");
    }

    PythonInterpreter interpreter = interpreters.take();
    List<List<Object>> data = DataUtils.dataFromTable(table, params.getPaths());
    if (data == null) {
      return Table.EMPTY_TABLE;
    }

    List<Object> args = params.getArgs();
    Map<String, Object> kvargs = params.getKwargs();

    List<List<Object>> res =
        (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs);

    if (res == null || res.size() < 3) {
      return Table.EMPTY_TABLE;
    }
    interpreters.add(interpreter);

    // [["key", col1, col2 ....],
    // ["LONG", type1, type2 ...],
    // [key1, val11, val21 ...],
    // [key2, val21, val22 ...]
    // ...]
    boolean hasKey = res.get(0).get(0).equals("key");
    if (hasKey) {
      res.get(0).remove(0);
      res.get(1).remove(0);
    }

    // if returns key, build header with key, and construct rows with key values
    Header header = RowUtils.constructHeaderWithFirstTwoRowsUsingFuncName(res, hasKey, funcName);
    return hasKey
        ? RowUtils.constructNewTableWithKey(header, res, 2)
        : RowUtils.constructNewTable(header, res, 2);
  }

  @Override
  public String getFunctionName() {
    return funcName;
  }
}
