package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public class PlanExecutor {

  private static final PhysicalEngine engine = PhysicalEngineImpl.getInstance();

  private static class PlanExecutorHolder {
    private static final PlanExecutor INSTANCE = new PlanExecutor();
  }

  public static PlanExecutor getInstance() {
    return PlanExecutorHolder.INSTANCE;
  }

  public RowStream execute(RequestContext ctx, Plan plan) throws PhysicalException {
    return engine.execute(ctx, plan.getRoot());
  }
}
