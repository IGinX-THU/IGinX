package cn.edu.tsinghua.iginx.engine.logical.optimizer.rbo;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.Map;

public class RBORuleCall extends RuleCall {

  public RBORuleCall(
      Operator subRoot,
      Map<Operator, Operator> parentIndexMap,
      Map<Operator, List<Operator>> childrenIndex) {
    super(subRoot, parentIndexMap, childrenIndex);
  }
}
