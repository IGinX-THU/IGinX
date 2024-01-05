package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import java.util.Collections;

public class CompletedFoldedPhysicalTask extends UnaryMemoryPhysicalTask {

  public CompletedFoldedPhysicalTask(PhysicalTask parentTask, RequestContext context) {
    super(Collections.emptyList(), parentTask, context);
  }
}
