package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.Collections;

public class IGinXPhysicalTask extends UnaryMemoryPhysicalTask {

  public IGinXPhysicalTask(Operator operator, RequestContext context) {
    super(Collections.singletonList(operator), null, context);
  }

  public Operator getOperator() {
    return getOperators().get(0);
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);
    visitor.leave();
  }
}
