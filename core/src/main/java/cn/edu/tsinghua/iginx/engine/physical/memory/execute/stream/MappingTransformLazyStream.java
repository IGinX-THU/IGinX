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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import java.util.List;

public class MappingTransformLazyStream extends UnaryLazyStream {

  private final List<FunctionCall> functionCallList;

  private RowStream resultStream;

  public MappingTransformLazyStream(MappingTransform transform, RowStream stream) {
    super(stream);
    this.functionCallList = transform.getFunctionCallList();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (resultStream == null) {
      resultStream = calculateResults();
    }
    return resultStream == null ? Header.EMPTY_HEADER : resultStream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (resultStream == null) {
      resultStream = calculateResults();
    }
    return resultStream != null && resultStream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return resultStream.next();
  }

  private RowStream calculateResults() throws PhysicalException {
    return RowUtils.calMappingTransform((Table) stream, functionCallList);
  }
}
