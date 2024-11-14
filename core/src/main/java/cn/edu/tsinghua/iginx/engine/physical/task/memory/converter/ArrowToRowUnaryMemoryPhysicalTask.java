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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.converter;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.Collections;

public class ArrowToRowUnaryMemoryPhysicalTask
    extends UnaryMemoryPhysicalTask<RowStream, BatchStream> {

  public ArrowToRowUnaryMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask, RequestContext context) {
    super(parentTask, Collections.emptyList(), context);
  }

  private volatile String info = "ArrowToRow";

  @Override
  public String getInfo() {
    return info;
  }

  @Override
  protected RowStream compute(BatchStream previous) throws PhysicalException {
    RowStream result = new BatchStreamToRowStreamWrapper(previous, getMetrics());
    try {
      info = "ArrowToRow: " + result.getHeader();
    } catch (PhysicalException e) {
      try (RowStream ignored = result) {
        throw e;
      }
    }
    return result;
  }

  @Override
  public Class<RowStream> getResultClass() {
    return RowStream.class;
  }
}
