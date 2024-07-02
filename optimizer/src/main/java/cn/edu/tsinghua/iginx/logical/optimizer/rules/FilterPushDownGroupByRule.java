/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class FilterPushDownGroupByRule extends Rule {
  private static List<OperatorType> validOps =
      Arrays.asList(OperatorType.GroupBy, OperatorType.Distinct);

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
     *     GroupBy/Distinct
     */
    super(
        "FilterPushDownGroupByRule",
        operand(Select.class, operand(AbstractUnaryOperator.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) call.getChildrenIndex().get(select).get(0);
    if (!validOps.contains(operator.getType())) {
      return false;
    }

    // 如果没有GroupBy Key，不能下推
    List<String> groupByCols = getGroupByCols(operator);
    if (groupByCols.isEmpty()) {
      return false;
    }

    // 分解Filter为一系列AND连接的子条件
    List<Filter> splitFilters = LogicalFilterUtils.splitFilter(select.getFilter());
    List<Filter> pushFilters = new ArrayList<>(), remainFilters = new ArrayList<>();
    for (Filter filter : splitFilters) {
      // 如果Filter中的列仅包含GroupBy Key，可以下推，否则不行
      if (new HashSet<>(groupByCols).containsAll(LogicalFilterUtils.getPathsFromFilter(filter))) {
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
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) call.getChildrenIndex().get(select).get(0);
    Select newSelect =
        new Select(operator.getSource(), new AndFilter(pushFilters), select.getTagFilter());
    operator.setSource(new OperatorSource(newSelect));

    if (remainFilters.isEmpty()) {
      call.transformTo(operator);
    } else {
      select.setFilter(new AndFilter(remainFilters));
      call.transformTo(select);
    }
  }

  private List<String> getGroupByCols(AbstractUnaryOperator operator) {
    if (operator.getType() == OperatorType.GroupBy) {
      return ((GroupBy) operator).getGroupByCols();
    } else if (operator.getType() == OperatorType.Distinct) {
      return ((Distinct) operator).getPatterns();
    }
    throw new IllegalArgumentException("OperatorType is not GroupBy or Distinct");
  }
}
