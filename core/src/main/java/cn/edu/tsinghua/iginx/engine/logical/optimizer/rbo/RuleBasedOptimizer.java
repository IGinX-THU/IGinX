package cn.edu.tsinghua.iginx.engine.logical.optimizer.rbo;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.Planner;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class RuleBasedOptimizer implements Optimizer {

  @Override
  public Operator optimize(Operator root) {
    Planner planner = new RuleBasedPlanner();
    planner.setRoot(root);
    return planner.findBest();
  }
}
