package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class PhysicalEngineMock implements PhysicalEngine {

  @Override
  public RowStream execute(RequestContext ctx, Operator root) throws PhysicalException {
    return null;
  }

  @Override
  public PhysicalOptimizer getOptimizer() {
    return null;
  }

  @Override
  public ConstraintManager getConstraintManager() {
    return null;
  }

  @Override
  public StoragePhysicalTaskExecutor getStoragePhysicalTaskExecutor() {
    return null;
  }

  @Override
  public StorageManager getStorageManager() {
    return null;
  }
}
