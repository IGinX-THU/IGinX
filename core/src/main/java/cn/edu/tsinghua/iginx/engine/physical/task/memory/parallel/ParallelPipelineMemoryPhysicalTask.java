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
import cn.edu.tsinghua.iginx.engine.physical.task.memory.*;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.arrow.util.Preconditions;

public class ParallelPipelineMemoryPhysicalTask
    extends MultiMemoryPhysicalTask<BatchStream, BatchStream> {

  public interface PipelineFactory {
    PipelineMemoryPhysicalTask createPipeline(
        RequestContext context, PhysicalTask<BatchStream> parentTask);
  }

  private final PipelineFactory pipelineFactory;
  private final int parallelism;
  private final PhysicalTask<BatchStream> parentTask;

  private List<GatherMemoryPhysicalTask> gathers;

  public ParallelPipelineMemoryPhysicalTask(
      PhysicalTask<BatchStream> parentTask,
      RequestContext context,
      PipelineFactory pipelineFactory,
      int parallelism) {
    super(Collections.emptyList(), context, Collections.singletonList(parentTask));
    this.parentTask = Objects.requireNonNull(parentTask);
    Preconditions.checkArgument(parallelism > 0);
    this.pipelineFactory = Objects.requireNonNull(pipelineFactory);
    this.parallelism = parallelism;
  }

  @Override
  public TaskResult<BatchStream> execute() {
    Future<TaskResult<BatchStream>> future = parentTask.getResult();
    try (TaskResult<BatchStream> parentResult = future.get()) {
      BatchStream stream = parentResult.unwrap();
      BatchStream result = compute(stream);
      return new TaskResult<>(result);
    } catch (PhysicalException e) {
      return new TaskResult<>(e);
    } catch (InterruptedException | ExecutionException e) {
      return new TaskResult<>(new PhysicalException(e));
    }
  }

  private BatchStream compute(BatchStream previous) {
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      gathers = new ArrayList<>();
      ScatterGatherBatchStream outputStream =
          new ScatterGatherBatchStream(previous, getMetrics(), parallelism);
      for (BatchStream branch : outputStream.getBranches()) {
        GatherMemoryPhysicalTask gather = constructPipeline(getContext(), branch, outputStream);
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
      RequestContext context, BatchStream stream, ScatterGatherBatchStream outputStream) {
    MemoryPhysicalTask<BatchStream> source =
        new StreamSourceMemoryPhysicalTask(context, "Parallel Pipeline Source", () -> stream);
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
    return "ParallelPipeline: " + parallelism + " pipelines";
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);
    parentTask.accept(visitor);
    if (gathers != null) {
      for (GatherMemoryPhysicalTask gather : gathers) {
        gather.accept(visitor);
      }
    }
    visitor.leave();
  }

  static class GatherMemoryPhysicalTask extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {

    private final ScatterGatherBatchStream outputStream;

    public GatherMemoryPhysicalTask(
        RequestContext context,
        PhysicalTask<BatchStream> parentTasks,
        ScatterGatherBatchStream outputStream) {
      super(parentTasks, Collections.emptyList(), context);
      this.outputStream = Objects.requireNonNull(outputStream);
    }

    @Override
    public TaskResult<BatchStream> execute() {
      Future<TaskResult<BatchStream>> future = parentTask.getResult();
      try (TaskResult<BatchStream> parentResult = future.get()) {
        outputStream.offer(parentResult, getMetrics());
        return new TaskResult<>(BatchStreams.empty());
      } catch (PhysicalException e) {
        return new TaskResult<>(e);
      } catch (InterruptedException | ExecutionException e) {
        return new TaskResult<>(new PhysicalException(e));
      }
    }

    @Override
    protected BatchStream compute(BatchStream previous) throws PhysicalException {
      throw new UnsupportedOperationException();
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
