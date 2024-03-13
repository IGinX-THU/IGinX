package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import java.util.logging.Logger;

public class ConstantFoldingRule extends Rule {
  private static final Logger LOGGER = Logger.getLogger(ConstantFoldingRule.class.getName());

  private static final class InstanceHolder {
    static final ConstantFoldingRule INSTANCE = new ConstantFoldingRule();
  }

  public static ConstantFoldingRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected ConstantFoldingRule() {
    /*
     * we want to match the topology like:
     *         SELECT
     *           |
     *          Any
     */
    super("ConstantFoldingRule", operand(Select.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RuleCall call) {}
}
