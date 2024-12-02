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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SetTransformLazyStream extends UnaryLazyStream {

  private final List<FunctionCall> functionCallList;

  private Row nextRow;

  private final Map<List<String>, RowStream> rowTransformStreamMap; // 用于存储参数为表达式的stream

  private final Map<List<String>, RowStream> distinctStreamMap; // 用于存储distinct处理过的stream

  private boolean hasConsumed = false;

  public SetTransformLazyStream(
      SetTransform setTransform,
      RowStream stream,
      Map<List<String>, RowStream> rowTransformStreamMap,
      Map<List<String>, RowStream> distinctStreamMap) {
    super(stream);
    this.rowTransformStreamMap = rowTransformStreamMap;
    this.distinctStreamMap = distinctStreamMap;
    this.functionCallList = setTransform.getFunctionCallList();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasConsumed && nextRow == null) { // 没有被消费过并且当前值为空，实际上表示当前值还没有被计算过
      nextRow = calculate();
      hasConsumed = nextRow == null;
    }
    return nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasConsumed && nextRow == null) { // 没有被消费过并且当前值为空，实际上表示当前值还没有被计算过
      nextRow = calculate();
      hasConsumed = nextRow == null;
    }
    return !hasConsumed;
  }

  private Row calculate() throws PhysicalException {
    SetMappingFunction function = null;
    List<Row> rowList = new ArrayList<>();
    try {
      for (FunctionCall functionCall : functionCallList) {
        function = (SetMappingFunction) functionCall.getFunction();
        FunctionParams params = functionCall.getParams();
        if (params.isDistinct()) {
          rowList.add(function.transform((Table) distinctStreamMap.get(params.getPaths()), params));
        } else if (functionCall.isNeedPreRowTransform()) {
          rowList.add(
              function.transform((Table) rowTransformStreamMap.get(params.getPaths()), params));
        } else {
          rowList.add(function.transform((Table) stream, params));
        }
      }

    } catch (Exception e) {
      if (function != null) {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute set mapping function " + function.getIdentifier() + ".",
            e);
      } else {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute set mapping function.", e);
      }
    }
    return combineMultipleColumns(rowList);
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    hasConsumed = true;
    return nextRow;
  }
}
