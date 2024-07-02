package cn.edu.tsinghua.iginx.engine.physical.optimizer;

import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface PhysicalOptimizer {

  PhysicalTask optimize(Operator root, RequestContext context);

  ConstraintManager getConstraintManager();

  ReplicaDispatcher getReplicaDispatcher();
}
