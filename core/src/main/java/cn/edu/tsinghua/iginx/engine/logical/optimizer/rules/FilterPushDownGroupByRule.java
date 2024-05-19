package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class FilterPushDownGroupByRule extends Rule {
  private static class InstanceHolder {
    private static final FilterPushDownGroupByRule INSTANCE = new FilterPushDownGroupByRule();
  }

  public static FilterPushDownGroupByRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownGroupByRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *         GroupBy
     */
    super("FilterPushDownGroupByRule", operand(Select.class, operand(GroupBy.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    GroupBy groupBy = (GroupBy) call.getChildrenIndex().get(select).get(0);
    // 如果没有GroupBy Key，不能下推
    if (groupBy.getGroupByCols().isEmpty()) {
      return false;
    }

    // 分解Filter为一系列AND连接的子条件
    List<Filter> splitFilters = LogicalFilterUtils.splitFilter(select.getFilter());
    List<Filter> pushFilters = new ArrayList<>(), remainFilters = new ArrayList<>();
    for (Filter filter : splitFilters) {
      // 如果Filter中的列仅包含GroupBy Key，可以下推，否则不行
      if (new HashSet<>(groupBy.getGroupByCols())
          .containsAll(LogicalFilterUtils.getPathsFromFilter(filter))) {
        pushFilters.add(filter);
      } else {
        remainFilters.add(filter);
      }
    }

    if (pushFilters.isEmpty()) {
      return false;
    }

    call.setContext(new Object[] {pushFilters, remainFilters});
    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    List<Filter> pushFilters = (List<Filter>) ((Object[]) call.getContext())[0];
    List<Filter> remainFilters = (List<Filter>) ((Object[]) call.getContext())[1];

    Select select = (Select) call.getMatchedRoot();
    GroupBy groupBy = (GroupBy) call.getChildrenIndex().get(select).get(0);
    Select newSelect =
        new Select(groupBy.getSource(), new AndFilter(pushFilters), select.getTagFilter());
    groupBy.setSource(new OperatorSource(newSelect));

    if (remainFilters.isEmpty()) {
      call.transformTo(groupBy);
    } else {
      select.setFilter(new AndFilter(remainFilters));
      call.transformTo(select);
    }
  }
}
