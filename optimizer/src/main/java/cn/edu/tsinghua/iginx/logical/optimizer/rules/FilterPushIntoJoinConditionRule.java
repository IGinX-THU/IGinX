package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.*;

public class FilterPushIntoJoinConditionRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(Arrays.asList(OperatorType.InnerJoin, OperatorType.CrossJoin));

  private static class FilterPushIntoJoinConditionRuleInstance {
    private static final FilterPushIntoJoinConditionRule INSTANCE =
        new FilterPushIntoJoinConditionRule();
  }

  public static FilterPushIntoJoinConditionRule getInstance() {
    return FilterPushIntoJoinConditionRuleInstance.INSTANCE;
  }

  protected FilterPushIntoJoinConditionRule() {
    /*
     * we want to match the topology like:
     *        Select
     *         |
     *   CrossJoin/InnerJoin
     */
    super(
        "FilterPushIntoJoinConditionRule",
        operand(Select.class, operand(AbstractJoin.class, any(), any())));
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
      innerJoin.setTagFilter(select.getTagFilter());
    }

    call.transformTo(join);
  }
}
