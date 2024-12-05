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

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;

@AutoService(Rule.class)
public class AggPushDownProjectReorderRule extends Rule {

  public AggPushDownProjectReorderRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         Project/Reorder
     */
    super(
        AggPushDownProjectReorderRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    // 如果project下面就是fragment，就不需要再下推了
    Operator root = call.getMatchedRoot();
    Operator child = call.getChildrenIndex().get(root).get(0);
    if (!OptimizerUtils.validateAggPushDown(root)) {
      return false;
    }
    if (child.getType() == OperatorType.Project) {
      Project project = (Project) child;
      return project.getSource().getType() != SourceType.Fragment;
    }

    return child.getType() == OperatorType.Reorder;
  }

  @Override
  public void onMatch(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    Operator child = call.getChildrenIndex().get(groupBy).get(0);

    List<String> newProjectPattern = new ArrayList<>(groupBy.getGroupByCols());
    groupBy
        .getFunctionCallList()
        .forEach(functionCall -> newProjectPattern.add(functionCall.getFunctionStr()));

    if (child.getType() == OperatorType.Project) {
      Project project = (Project) child;
      project.setPatterns(newProjectPattern);
    } else if (child.getType() == OperatorType.Reorder) {
      Reorder reorder = (Reorder) child;
      reorder.setPatterns(newProjectPattern);
    }

    groupBy.setSource(((UnaryOperator) child).getSource());
    ((UnaryOperator) child).setSource(new OperatorSource(groupBy));

    call.transformTo(child);
  }
}
