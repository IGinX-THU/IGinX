package cn.edu.tsinghua.iginx.logical.optimizer.rbo;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.List;
import java.util.Map;

public class RBORuleCall extends RuleCall {

  public RBORuleCall(
      Operator subRoot,
      Map<Operator, Operator> parentIndexMap,
      Map<Operator, List<Operator>> childrenIndex,
      RuleBasedPlanner planner) {
    super(subRoot, parentIndexMap, childrenIndex, planner);
  }
}
