package cn.edu.tsinghua.iginx.engine.distributedquery.worker;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.ResultUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.utils.FastjsonSerializeUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubPlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(SubPlanExecutor.class);

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private final AtomicBoolean hasLoadFragments = new AtomicBoolean(false);

  private static class SubPlanExecutorHolder {
    private static final SubPlanExecutor INSTANCE = new SubPlanExecutor();
  }

  public static SubPlanExecutor getInstance() {
    return SubPlanExecutorHolder.INSTANCE;
  }

  public void execute(RequestContext context) {
    if (!hasLoadFragments.get()) {
      metaManager.createInitialFragmentsAndStorageUnits(null, null);
      hasLoadFragments.set(true);
    }

    Operator subPlan = FastjsonSerializeUtils.deserialize(context.getSubPlanMsg(), Operator.class);
    PhysicalEngine engine = PhysicalEngineImpl.getInstance();

    RowStream rowStream = null;
    try {
      rowStream = engine.execute(context, subPlan);
    } catch (PhysicalException e) {
      logger.error("execute sub plan failed, because: ", e);
    }

    if (rowStream == null) {
      context.setResult(new Result(RpcUtils.FAILURE));
      return;
    }

    try {
      ResultUtils.setResultFromRowStream(context, rowStream);
    } catch (PhysicalException e) {
      logger.error("read data from row stream error, because, ", e);
      context.setResult(new Result(RpcUtils.FAILURE));
    }
  }
}
