package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

public class FilterPushDownSelectRule extends Rule {
  private static final class InstanceHolder {
    static final FilterPushDownSelectRule INSTANCE = new FilterPushDownSelectRule();
  }

  public static FilterPushDownSelectRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownSelectRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *         Select
     */
    super("FilterPushDownSelectRule", operand(Select.class, operand(Select.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    // 应当把两个Select合并，用and连接
    Select select = (Select) call.getMatchedRoot();
    Select childSelect = (Select) call.getChildrenIndex().get(select).get(0);

    childSelect.setFilter(
        LogicalFilterUtils.mergeFilter(select.getFilter(), childSelect.getFilter()));
    call.transformTo(childSelect);
  }
}
