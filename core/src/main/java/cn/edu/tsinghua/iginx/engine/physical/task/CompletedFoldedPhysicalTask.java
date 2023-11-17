package cn.edu.tsinghua.iginx.engine.physical.task;

import java.util.Collections;

public class CompletedFoldedPhysicalTask extends UnaryMemoryPhysicalTask {

  public CompletedFoldedPhysicalTask(PhysicalTask parentTask) {
    super(Collections.emptyList(), parentTask);
  }

  @Override
  public TaskExecuteResult execute() {
    return getParentTask().getResult();
  }
}
