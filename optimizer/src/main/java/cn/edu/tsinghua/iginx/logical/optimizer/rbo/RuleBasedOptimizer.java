package cn.edu.tsinghua.iginx.logical.optimizer.rbo;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.Planner;

public class RuleBasedOptimizer implements Optimizer {

  private static final class InstanceHolder {
    static final RuleBasedOptimizer instance = new RuleBasedOptimizer();
  }

  public static RuleBasedOptimizer getInstance() {
    return InstanceHolder.instance;
  }

  @Override
  public Operator optimize(Operator root) {
    Planner planner = new RuleBasedPlanner();
    planner.setRoot(root);
    return planner.findBest();
  }
}
