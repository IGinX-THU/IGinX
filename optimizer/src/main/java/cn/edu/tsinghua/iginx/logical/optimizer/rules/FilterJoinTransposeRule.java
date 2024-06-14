package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

public class FilterJoinTransposeRule extends Rule {

  private static final class InstanceHolder {
    static final FilterJoinTransposeRule INSTANCE = new FilterJoinTransposeRule();
  }

  public static FilterJoinTransposeRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterJoinTransposeRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *      AbstractJoin
     *        /     \
     *     Any       Any
     */
    super(
        "FilterJoinTransposeRule",
        operand(Select.class, operand(AbstractJoin.class, any(), any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);
    // we only deal with cross/inner/outer join for now.
    return !(join instanceof MarkJoin) && !(join instanceof SingleJoin);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);

    Source sourceA = join.getSourceA();
    Source sourceB = join.getSourceB();

    Select selectA = (Select) select.copyWithSource(sourceA);
    Select selectB = (Select) select.copyWithSource(sourceB);
    AbstractJoin newJoin =
        (AbstractJoin)
            join.copyWithSource(new OperatorSource(selectA), new OperatorSource(selectB));
    call.transformTo(newJoin);
  }
}
