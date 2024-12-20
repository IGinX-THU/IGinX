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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import java.util.List;
import java.util.Map;

public class PyUDAF extends PyUDF implements UDAF {

  private static final String PY_UDAF = "py_udaf";

  private final String funcName;

  public PyUDAF(String funcName, String moduleName, String className) {
    super(moduleName, className);
    this.funcName = funcName;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.UDF;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.SetMapping;
  }

  @Override
  public String getIdentifier() {
    return PY_UDAF;
  }

  @Override
  public Row transform(Table table, FunctionParams params) throws Exception {
    if (!CheckUtils.isLegal(params)) {
      throw new IllegalArgumentException("unexpected params for PyUDAF.");
    }

    List<List<Object>> data = DataUtils.dataFromTable(table, params.getPaths());
    if (data == null) {
      return Row.EMPTY_ROW;
    }

    List<Object> args = params.getArgs();
    Map<String, Object> kvargs = params.getKwargs();

    List<List<Object>> res = invokePyUDF(data, args, kvargs);

    if (res == null || res.size() < 3) {
      return Row.EMPTY_ROW;
    }

    // [["key", col1, col2 ....],
    // ["LONG", type1, type2 ...],
    // [key1, val11, val21 ...]]
    boolean hasKey = res.get(0).get(0).equals("key");
    long key = -1;
    if (hasKey) {
      res.get(0).remove(0);
      res.get(1).remove(0);
      key = (Long) res.get(2).remove(0);
    }

    Header header = RowUtils.constructHeaderWithFirstTwoRowsUsingFuncName(res, hasKey, funcName);
    return hasKey
        ? RowUtils.constructNewRowWithKey(header, key, res.get(2))
        : RowUtils.constructNewRow(header, res.get(2));
  }

  @Override
  public String getFunctionName() {
    return funcName;
  }
}
