package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterPushDownProjectReorderSortRule extends Rule {

  private static final Set<Class> validOps =
      new HashSet<>(Arrays.asList(Project.class, Reorder.class, Sort.class));

  private static final class InstanceHolder {
    static final FilterPushDownProjectReorderSortRule INSTANCE =
        new FilterPushDownProjectReorderSortRule();
  }

  public static FilterPushDownProjectReorderSortRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownProjectReorderSortRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *    Project/Reorder/Sort
     */
    super(
        "FilterPushDownProjectReorderSortRule",
        operand(Select.class, operand(AbstractUnaryOperator.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    return validOps.contains(operator.getClass())
        && operator.getSource().getType() != SourceType.Fragment;
  }

  @Override
  public void onMatch(RuleCall call) {
    // 应该没什么要注意的，单纯交换位置即可
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    select.setSource(operator.getSource());
    operator.setSource(new OperatorSource(select));

    call.transformTo(operator);
  }
}
