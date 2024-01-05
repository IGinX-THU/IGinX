package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.removeNot;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;

public class RemoveNotRule extends Rule {

  private static final class InstanceHolder {
    static final RemoveNotRule INSTANCE = new RemoveNotRule();
  }

  public static RemoveNotRule getInstance() {
    return RemoveNotRule.InstanceHolder.INSTANCE;
  }

  protected RemoveNotRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *          Any
     */
    super("RemoveNotRule", operand(Select.class, any()));
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    select.setFilter(removeNot(select.getFilter()));
  }
}
