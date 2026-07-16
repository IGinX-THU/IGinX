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
package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalSupplier;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Objects;

public class StreamSourceMemoryPhysicalTask extends SourceMemoryPhysicalTask {

  private final PhysicalSupplier<BatchStream> supplier;

  public StreamSourceMemoryPhysicalTask(
      RequestContext context, Object info, PhysicalSupplier<BatchStream> supplier) {
    super(context, info);
    this.supplier = Objects.requireNonNull(supplier);
  }

  @Override
  public TaskResult<BatchStream> execute() {
    try {
      return new TaskResult<>(new StreamWrapper(supplier.get()));
    } catch (PhysicalException e) {
      return new TaskResult<>(e);
    }
  }

  private class StreamWrapper implements BatchStream {

    private final BatchStream stream;

    private StreamWrapper(BatchStream stream) {
      this.stream = stream;
    }

    @Override
    public BatchSchema getSchema() throws PhysicalException {
      return stream.getSchema();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      return stream.hasNext();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      Batch batch = stream.getNext();
      getMetrics().accumulateAffectRows(batch.getRowCount());
      return batch;
    }

    @Override
    public void close() throws PhysicalException {
      stream.close();
    }
  }
}
