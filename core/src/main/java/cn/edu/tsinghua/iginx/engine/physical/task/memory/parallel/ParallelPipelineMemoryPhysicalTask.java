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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.*;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.util.Preconditions;

public class ParallelPipelineMemoryPhysicalTask
    extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

  public interface PipelineFactory {
    PipelineMemoryPhysicalTask createPipeline(
        RequestContext context, PhysicalTask<BatchStream> parentTask);
  }

  private final PipelineFactory pipelineFactory;
  private final int parallelism;

  private List<GatherMemoryPhysicalTask> gathers;

  public ParallelPipelineMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask,
      RequestContext context,
      PipelineFactory pipelineFactory,
      int parallelism) {
    super(parentTask, Collections.emptyList(), context);
    Preconditions.checkArgument(parallelism > 0);
    this.pipelineFactory = Objects.requireNonNull(pipelineFactory);
    this.parallelism = parallelism;
  }

  @Override
  protected BatchStream compute(BatchStream previous) {
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      ScatterBatchStream scatterStream = new ScatterBatchStream(previous);
      GatherBatchStream outputStream = new GatherBatchStream(scatterStream, getMetrics());
      gathers = new ArrayList<>(parallelism);
      for (int i = 0; i < parallelism; i++) {
        GatherMemoryPhysicalTask gather =
            constructPipeline(getContext(), scatterStream, outputStream);
        gathers.add(gather);
      }
      if (getFollowerTask() != null) {
        SourceMemoryPhysicalTask sourceTask =
            new EmptySourceMemoryPhysicalTask(getContext(), "Parallel Pipeline Target");
        sourceTask.setFollowerTask(getFollowerTask());
        getContext().getPhysicalEngine().submit(sourceTask);
      }
      for (GatherMemoryPhysicalTask gather : gathers) {
        if (gather == gathers.get(0)) {
          SourceMemoryPhysicalTask sourceTask = getSourceTask(gather);
          setFollowerTask(sourceTask);
        } else {
          getContext().getPhysicalEngine().submit(gather);
        }
      }
      return outputStream;
    }
  }

  private GatherMemoryPhysicalTask constructPipeline(
      RequestContext context, BatchStream stream, GatherBatchStream outputStream) {
    MemoryPhysicalTask<BatchStream> source =
        new StreamSourceMemoryPhysicalTask(context, "Parallel Pipeline Source", stream);
    PipelineMemoryPhysicalTask pipeline = pipelineFactory.createPipeline(context, source);
    GatherMemoryPhysicalTask gather = new GatherMemoryPhysicalTask(context, pipeline, outputStream);

    PhysicalTask<?> follower = gather;
    PipelineMemoryPhysicalTask current = pipeline;
    while (true) {
      current.setFollowerTask(follower);
      follower = current;
      PhysicalTask<BatchStream> next = current.getParentTask();
      if (next instanceof PipelineMemoryPhysicalTask) {
        current = (PipelineMemoryPhysicalTask) current.getParentTask();
      } else if (next == source) {
        break;
      } else {
        throw new IllegalStateException("task is not a pipeline task chain");
      }
    }
    source.setFollowerTask(current);

    return gather;
  }

  private StreamSourceMemoryPhysicalTask getSourceTask(GatherMemoryPhysicalTask gather) {
    PipelineMemoryPhysicalTask task = (PipelineMemoryPhysicalTask) gather.getParentTask();
    while (task.getParentTask() instanceof PipelineMemoryPhysicalTask) {
      task = (PipelineMemoryPhysicalTask) task.getParentTask();
    }
    return (StreamSourceMemoryPhysicalTask) task.getParentTask();
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public String getInfo() {
    return "ParallelPipeline: 1 source and " + parallelism + " pipelines";
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.visit(this);
    if (gathers != null) {
      for (GatherMemoryPhysicalTask gather : gathers) {
        gather.accept(visitor);
      }
    }
    PhysicalTask<?> task = getParentTask();
    if (task != null) {
      task.accept(visitor);
    }
  }

  static class GatherMemoryPhysicalTask extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

    private final GatherBatchStream outputStream;

    public GatherMemoryPhysicalTask(
        RequestContext context,
        PhysicalTask<BatchStream> parentTasks,
        GatherBatchStream outputStream) {
      super(parentTasks, Collections.emptyList(), context);
      this.outputStream = Objects.requireNonNull(outputStream);
    }

    @Override
    protected BatchStream compute(BatchStream previous) throws PhysicalException {
      try (BatchStream ignored = previous) {
        outputStream.offer(previous, getMetrics());
        return BatchStreams.empty();
      }
    }

    @Override
    public Class<BatchStream> getResultClass() {
      return BatchStream.class;
    }

    @Override
    public String getInfo() {
      return "Parallel Pipeline Gather";
    }
  }
}
