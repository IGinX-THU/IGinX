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

import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;
import java.util.*;

@AutoService(Rule.class)
public class AggPushDownSortRule extends Rule {

  public AggPushDownSortRule() {
    /*
     * we want to match the topology like:
     *         GroupBy/Set Transform
     *           |
     *          Sort
     */
    super(AggPushDownSortRule.class.getName(), "AggPushDownRule", any(),2, RuleStrategy.FIXED_POINT);
  }

  private static final Set<String> ALLOWED_FUNCTIONS =
      new HashSet<>(Arrays.asList("MAX", "MIN", "AVG", "SUM", "COUNT"));

  @Override
  public boolean matches(RuleCall call) {
    // 如果只包含几个不依赖顺序的函数，可以下推
    Operator root = call.getMatchedRoot();
    if (!OptimizerUtils.validateAggPushDown(root)) {
      return false;
    }
    if (call.getChildrenIndex().get(root) == null
        || call.getChildrenIndex().get(root).size() != 1) {
      return false;
    }
    Operator child = call.getChildrenIndex().get(root).get(0);
    if (child.getType() != OperatorType.Sort) {
      return false;
    }

    if (root.getType() == OperatorType.GroupBy || root.getType() == OperatorType.SetTransform) {
      return OperatorUtils.getFunctionCallList(root).stream()
          .allMatch(
              fc -> ALLOWED_FUNCTIONS.contains(fc.getFunction().getIdentifier().toUpperCase()));
    }

    return false;
  }

  @Override
  public void onMatch(RuleCall call) {
    UnaryOperator root = (UnaryOperator) call.getMatchedRoot();
    Operator child = call.getChildrenIndex().get(root).get(0);

    // group by会打乱sort排布的顺序，此处的sort没有作用，可以直接去掉
    root.setSource(((UnaryOperator) child).getSource());
    call.transformTo(root);
  }
}
