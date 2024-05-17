package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterPushIntoJoinConditionRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(Arrays.asList(OperatorType.InnerJoin, OperatorType.OuterJoin));

  private static class FilterPushIntoJoinConditionRuleInstance {
    private static final FilterPushIntoJoinConditionRule INSTANCE =
        new FilterPushIntoJoinConditionRule();
  }

  public FilterPushIntoJoinConditionRule getInstance() {
    return FilterPushIntoJoinConditionRuleInstance.INSTANCE;
  }

  protected FilterPushIntoJoinConditionRule() {
    /*
     * we want to match the topology like:
     *        Select
     *         |
     *   CrossJoin/InnerJoin
     */
    super("FilterPushIntoJoinCondition", operand(Select.class, operand(AbstractJoin.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);

    // select条件一定可以推进InnerJoin和CrossJoin的Join条件中，但不能推进OuterJoin里。
    return validOps.contains(join.getType());
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);

    if (join.getType() == OperatorType.CrossJoin) {
      // 如果是CrossJoin，可以通过添加filter转换为InnerJoin
      join =
          new InnerJoin(
              join.getSourceA(),
              join.getSourceB(),
              join.getPrefixA(),
              join.getPrefixB(),
              select.getFilter(),
              new ArrayList<>());
      ((InnerJoin) join).reChooseJoinAlg();
    } else if (join.getType() == OperatorType.InnerJoin) {
      // 如果是InnerJoin，直接将filter推进去
      InnerJoin innerJoin = (InnerJoin) join;
      innerJoin.setFilter(FilterUtils.combineTwoFilter(innerJoin.getFilter(), select.getFilter()));
      innerJoin.reChooseJoinAlg();
    }

    call.transformTo(join);
  }
}
