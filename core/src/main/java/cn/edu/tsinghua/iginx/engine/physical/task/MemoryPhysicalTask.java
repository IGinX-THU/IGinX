package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MemoryPhysicalTask extends AbstractPhysicalTask {

  protected AtomicInteger parentReadyCount;

  public MemoryPhysicalTask(TaskType type, List<Operator> operators, RequestContext context) {
    super(type, operators, context);
    parentReadyCount = new AtomicInteger(0);
  }

  public abstract TaskExecuteResult execute(); // 在 parent 都完成执行后，可以执行该任务

  public abstract boolean notifyParentReady(); // 通知当前任务的某个父节点已经完成，该方法会返回 boolean 值，表示当前的任务是否可以执行
}
