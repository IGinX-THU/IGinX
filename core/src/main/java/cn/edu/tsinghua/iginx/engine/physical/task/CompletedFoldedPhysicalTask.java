package cn.edu.tsinghua.iginx.engine.physical.task;

import java.util.Collections;

public class CompletedFoldedPhysicalTask extends MemoryPhysicalTask {

  private PhysicalTask parentTask;

  public CompletedFoldedPhysicalTask(PhysicalTask parentTask) {
    super(TaskType.CompletedFolded, Collections.emptyList());
    this.parentTask = parentTask;
  }

  public PhysicalTask getParentTask() {
    return parentTask;
  }

  public void setParentTask(PhysicalTask parentTask) {
    this.parentTask = parentTask;
  }

  @Override
  public TaskExecuteResult execute() {
    return parentTask.getResult();
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == 1;
  }
}
