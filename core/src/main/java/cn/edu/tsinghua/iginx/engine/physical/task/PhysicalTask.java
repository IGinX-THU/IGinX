package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public interface PhysicalTask extends Measurable {

  void accept(TaskVisitor visitor);

  TaskType getType();

  List<Operator> getOperators();

  TaskExecuteResult getResult();

  void setResult(TaskExecuteResult result);

  PhysicalTask getFollowerTask();

  void setFollowerTask(PhysicalTask task);

  String getInfo();
}
