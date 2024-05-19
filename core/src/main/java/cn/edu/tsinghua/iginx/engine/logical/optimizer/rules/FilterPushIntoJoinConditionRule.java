package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.*;
import java.util.stream.Collectors;

public class FilterPushIntoJoinConditionRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(
          Arrays.asList(
              OperatorType.InnerJoin,
              OperatorType.CrossJoin,
              OperatorType.MarkJoin,
              OperatorType.SingleJoin));

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
    super("FilterPushIntoJoinConditionRule", operand(Select.class, operand(AbstractJoin.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);

    // markjoin上的mark列filter不能推进条件
    if (join.getType() == OperatorType.MarkJoin) {
      Filter filter = select.getFilter();
      if (filter.getType() == FilterType.Value
          && ((ValueFilter) filter).getPath().startsWith(MarkJoin.MARK_PREFIX)) {
        return false;
      }
    }
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
    } else if (join.getType() == OperatorType.InnerJoin
        || join.getType() == OperatorType.SingleJoin) {
      // 如果是InnerJoin\SingleJoin，直接将filter推进去
      InnerJoin innerJoin = (InnerJoin) join;
      innerJoin.setFilter(FilterUtils.combineTwoFilter(innerJoin.getFilter(), select.getFilter()));
      innerJoin.reChooseJoinAlg();
    } else if (join.getType() == OperatorType.MarkJoin) {
      // 如果是MarkJoin，将除了mark filter之外的filter推进去
      MarkJoin markJoin = (MarkJoin) join;
      List<Filter> splitFilter =
          LogicalFilterUtils.splitFilter(select.getFilter()).stream()
              .filter(
                  filter ->
                      filter.getType() != FilterType.Value
                          || !((ValueFilter) filter).getPath().startsWith(MarkJoin.MARK_PREFIX))
              .collect(Collectors.toList());
      if (!splitFilter.isEmpty()) {
        markJoin.setFilter(
            FilterUtils.combineTwoFilter(markJoin.getFilter(), new AndFilter(splitFilter)));
      }
    }

    call.transformTo(join);
  }
}
