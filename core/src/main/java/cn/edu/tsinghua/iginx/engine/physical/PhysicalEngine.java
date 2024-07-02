package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface PhysicalEngine {

  RowStream execute(RequestContext ctx, Operator root) throws PhysicalException;

  PhysicalOptimizer getOptimizer();

  ConstraintManager getConstraintManager();

  StoragePhysicalTaskExecutor getStoragePhysicalTaskExecutor();

  StorageManager getStorageManager();
}
