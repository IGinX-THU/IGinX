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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.row;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.Collections;

public class RowToArrowUnaryMemoryPhysicalTask
    extends UnaryMemoryPhysicalTask<BatchStream, RowStream> {

  public RowToArrowUnaryMemoryPhysicalTask(
      PhysicalTask<RowStream> parentTask, RequestContext context) {
    super(parentTask, Collections.emptyList(), context);
  }

  private volatile String info = "RowToArrow";

  @Override
  public String getInfo() {
    return info;
  }

  @Override
  protected BatchStream compute(RowStream previous) throws PhysicalException {
    BatchStream result =
        new RowStreamToBatchStreamWrapper(
            getContext().getAllocator(), previous, getMetrics(), getContext().getBatchRowCount());
    try {
      info = "RowToArrow: " + result.getSchema();
    } catch (PhysicalException e) {
      try (BatchStream ignored = result) {
        throw e;
      }
    }

    return result;
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }
}
