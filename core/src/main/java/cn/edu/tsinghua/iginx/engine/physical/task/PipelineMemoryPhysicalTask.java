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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline.PipelineExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Objects;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
public class PipelineMemoryPhysicalTask extends UnaryMemoryPhysicalTask {

  private final PipelineExecutor executor;
  private final ExecutorContext context;

  public PipelineMemoryPhysicalTask(
      PhysicalTask parentTask, RequestContext context, PipelineExecutor executor) {
    super(parentTask, context);
    this.executor = Objects.requireNonNull(executor);
    this.context = new PhysicalTaskExecutorContext(this);
  }

  @Override
  public String getInfo() {
    return "Pipeline of " + executor.getDescription();
  }

  @Override
  protected BatchStream compute(@WillClose BatchStream previous) throws PhysicalException {
    try {
      executor.internalInitialize(context, previous.getSchema());
    } catch (Throwable e) {
      try {
        previous.close();
      } catch (Throwable e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }
    return new PipelineBatchStream(previous, executor);
  }

  private static class PipelineBatchStream implements BatchStream {

    private final BatchStream source;
    private final PipelineExecutor executor;

    public PipelineBatchStream(@WillCloseWhenClosed BatchStream source, PipelineExecutor executor) {
      this.source = Objects.requireNonNull(source);
      this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public BatchSchema getSchema() {
      return executor.getOutputSchema();
    }

    @Override
    public Batch getNext() throws PhysicalException {
      Batch sourceNext = source.getNext();
      if (sourceNext == null) {
        return null;
      }
      return executor.compute(sourceNext);
    }

    @Override
    public void close() throws PhysicalException {
      try (BatchStream source = this.source;
          PipelineExecutor executor = this.executor) {
        // Do nothing
      }
    }
  }
}
