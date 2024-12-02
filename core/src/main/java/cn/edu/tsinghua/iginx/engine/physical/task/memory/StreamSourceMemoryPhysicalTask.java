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
package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Objects;

public class StreamSourceMemoryPhysicalTask extends SourceMemoryPhysicalTask {

  private final BatchStream stream;

  public StreamSourceMemoryPhysicalTask(RequestContext context, Object info, BatchStream stream) {
    super(context, info);
    this.stream = Objects.requireNonNull(stream);
  }

  @Override
  public TaskResult<BatchStream> execute() {
    return new TaskResult<>(new StreamWrapper(stream));
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
