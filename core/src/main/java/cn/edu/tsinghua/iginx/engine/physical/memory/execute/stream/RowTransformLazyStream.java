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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import java.util.ArrayList;
import java.util.List;

public class RowTransformLazyStream extends UnaryLazyStream {

  private final List<FunctionCall> functionCallList;

  private Row nextRow;

  private Header header;

  public RowTransformLazyStream(RowTransform rowTransform, RowStream stream) {
    super(stream);
    this.functionCallList = new ArrayList<>();
    rowTransform
        .getFunctionCallList()
        .forEach(
            functionCall -> {
              if (functionCall == null || functionCall.getFunction() == null) {
                throw new IllegalArgumentException("function shouldn't be null");
              }
              if (functionCall.getFunction().getMappingType() != MappingType.RowMapping) {
                throw new IllegalArgumentException("function should be set mapping function");
              }
              this.functionCallList.add(functionCall);
            });
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      if (nextRow == null) {
        nextRow = calculateNext();
      }
      header = nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
    }
    return header;
  }

  private Row calculateNext() throws PhysicalException {
    while (stream.hasNext()) {
      Row row = RowUtils.calRowTransform(stream.next(), functionCallList, false);
      if (!row.equals(Row.EMPTY_ROW)) {
        return row;
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }
}
