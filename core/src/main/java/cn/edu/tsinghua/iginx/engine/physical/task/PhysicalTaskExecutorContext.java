package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;

public class PhysicalTaskExecutorContext implements ExecutorContext {

  private final PhysicalTask task;

  public PhysicalTaskExecutorContext(PhysicalTask task) {
    this.task = Objects.requireNonNull(task);
  }

  @Override
  public BufferAllocator getAllocator() {
    return task.getContext().getAllocator();
  }

  @Override
  public void addWarningMessage(String message) {
    task.getContext().addWarningMessage(message);
  }

  @Override
  public void accumulateCpuTime(long millis) {
    task.getMetrics().accumulateCpuTime(millis);
  }
}
