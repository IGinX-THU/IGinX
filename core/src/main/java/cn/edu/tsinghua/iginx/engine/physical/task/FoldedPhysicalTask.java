package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoldedPhysicalTask extends MemoryPhysicalTask {

  private static final Logger logger = LoggerFactory.getLogger(FoldedPhysicalTask.class);

  private final Operator foldedRoot;

  private final List<PhysicalTask> parentTasks;

  public FoldedPhysicalTask(
      List<Operator> operators, Operator foldedRoot, List<PhysicalTask> parentTasks) {
    super(TaskType.Folded, operators);
    this.foldedRoot = foldedRoot;
    this.parentTasks = parentTasks;
  }

  public Operator getFoldedRoot() {
    return foldedRoot;
  }

  public List<PhysicalTask> getParentTasks() {
    return parentTasks;
  }

  @Override
  public TaskExecuteResult execute() {
    List<TaskExecuteResult> parentResults = new ArrayList<>();
    for (PhysicalTask parentTask : parentTasks) {
      TaskExecuteResult parentResult = parentTask.getResult();
      if (parentResult == null) {
        return new TaskExecuteResult(
            new PhysicalException("unexpected parent task execute result for " + this + ": null"));
      }
      if (parentResult.getException() != null) {
        return parentResult;
      }
      parentResults.add(parentResult);
    }

    return null;
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == parentTasks.size();
  }

  private Operator reGenerateRoot(Operator root) {
    return null;
  }
}
