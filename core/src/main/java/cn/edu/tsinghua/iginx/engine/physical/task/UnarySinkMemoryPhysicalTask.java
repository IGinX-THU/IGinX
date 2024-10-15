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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.UnarySinkExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;

public class UnarySinkMemoryPhysicalTask extends UnaryMemoryPhysicalTask {

  private final UnarySinkExecutor executor;

  public UnarySinkMemoryPhysicalTask(
      PhysicalTask parentTask, RequestContext context, UnarySinkExecutor executor) {
    super(parentTask, context);
    this.executor = Objects.requireNonNull(executor);
  }

  @Override
  public String getInfo() {
    return executor.getDescription();
  }

  @Override
  protected BatchStream compute(BatchStream previous) throws PhysicalException {
    try (BatchStream previousHolder = previous) {
      executor.initialize(executorContext, previous.getSchema());
      while (true) {
        try (Batch batch = previousHolder.getNext()) {
          if (batch == null) {
            break;
          }
          executor.consume(batch);
        }
      }
      executor.finish();
      return new UnarySinkBatchStream(executor);
    }
  }

  private static class UnarySinkBatchStream implements BatchStream {

    private final UnarySinkExecutor executor;

    public UnarySinkBatchStream(@WillCloseWhenClosed UnarySinkExecutor executor) {
      this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public BatchSchema getSchema() {
      return executor.getOutputSchema();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      if (executor.canProduce()) {
        return executor.produce();
      }
      return null;
    }

    @Override
    public void close() throws PhysicalException {
      executor.close();
    }
  }
}
