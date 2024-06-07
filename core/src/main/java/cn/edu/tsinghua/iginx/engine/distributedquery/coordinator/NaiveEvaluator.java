package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.FragmentsVisitor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;

public class NaiveEvaluator implements Evaluator {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static class NaiveEvaluatorHolder {
    private static final NaiveEvaluator INSTANCE = new NaiveEvaluator();
  }

  public static NaiveEvaluator getInstance() {
    return NaiveEvaluatorHolder.INSTANCE;
  }

  @Override
  public boolean needDistributedQuery(Operator root) {
    FragmentsVisitor visitor = new FragmentsVisitor();
    root.accept(visitor);
    int fragmentSize = visitor.getFragmentCount();
    int iginxSize = metaManager.getIginxList().size();
    return fragmentSize > config.getDistributedQueryTriggerThreshold()
        && fragmentSize >= iginxSize
        && iginxSize > 1;
  }
}
