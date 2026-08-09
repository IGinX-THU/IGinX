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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.EmptySourceMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.SourceMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import java.util.Collections;
import java.util.Objects;
import javax.annotation.WillClose;

public class FetchAsyncMemoryPhysicalTask
    extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

  public FetchAsyncMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask, RequestContext context) {
    super(parentTask, Collections.emptyList(), context);
  }

  @Override
  protected BatchStream compute(@WillClose BatchStream previous) {
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      FetchAsyncBatchStream stream = new FetchAsyncBatchStream(previous);
      if (getFollowerTask() != null) {
        SourceMemoryPhysicalTask sourceTask =
            new EmptySourceMemoryPhysicalTask(getContext(), "FetchAsyncSource");
        sourceTask.setFollowerTask(getFollowerTask());
        getContext().getPhysicalEngine().submit(sourceTask);
      }
      setFollowerTask(new BatchFetcher(stream));
      return stream;
    }
  }

  @Override
  public String getInfo() {
    return "FetchAsync";
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public void setResult(TaskResult<?> result) {
    super.setResult(result);
  }

  private class BatchFetcher extends MemoryPhysicalTask<BatchStream> {

    private final FetchAsyncBatchStream exchangeStream;

    public BatchFetcher(FetchAsyncBatchStream exchangeStream) {
      super(
          TaskType.UnaryMemory,
          Collections.emptyList(),
          FetchAsyncMemoryPhysicalTask.this.getContext(),
          1);
      this.exchangeStream = Objects.requireNonNull(exchangeStream);
    }

    @Override
    public void accept(TaskVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getInfo() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<BatchStream> getResultClass() {
      return BatchStream.class;
    }

    @Override
    public TaskResult<BatchStream> execute() {
      try {
        exchangeStream.fetchAll(FetchAsyncMemoryPhysicalTask.this.getMetrics());
      } catch (PhysicalException e) {
        return new TaskResult<>(e);
      }
      return new TaskResult<>(BatchStreams.empty());
    }
  }
}
