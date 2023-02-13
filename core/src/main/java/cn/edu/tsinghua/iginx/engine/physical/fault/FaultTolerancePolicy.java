package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;

public interface FaultTolerancePolicy {

    void persistence(PhysicalTask task, TaskExecuteResult result);

}
