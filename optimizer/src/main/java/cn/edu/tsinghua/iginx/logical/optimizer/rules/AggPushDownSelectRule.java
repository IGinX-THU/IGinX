/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@AutoService(Rule.class)
public class AggPushDownSelectRule extends Rule {

  public AggPushDownSelectRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         Select
     */
    super(
        AggPushDownSelectRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, operand(Select.class, any())),2, RuleStrategy.FIXED_POINT);
  }

  @Override
  public boolean matches(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    Select select = (Select) call.getChildrenIndex().get(groupBy).get(0);
    if (!OptimizerUtils.validateAggPushDown(groupBy)) {
      return false;
    }
    // 如果select下面就是fragment，就不需要再下推了
    Operator selectChild = call.getChildrenIndex().get(select).get(0);
    if (selectChild.getType() == OperatorType.Project
        && ((Project) selectChild).getSource().getType() == SourceType.Fragment) {
      return false;
    }

    // select中涉及的列仅包含group by列，才能下推
    List<String> groupByCols = groupBy.getGroupByCols();
    Set<String> selectPaths = LogicalFilterUtils.getPathsFromFilter(select.getFilter());

    return new HashSet<>(groupByCols).containsAll(selectPaths);
  }

  @Override
  public void onMatch(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    Select select = (Select) call.getChildrenIndex().get(groupBy).get(0);

    groupBy.setSource(select.getSource());
    select.setSource(new OperatorSource(groupBy));
  }
}
