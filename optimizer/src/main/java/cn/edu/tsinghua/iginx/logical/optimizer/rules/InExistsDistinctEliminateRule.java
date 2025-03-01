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

import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

/**
 * 该类实现了Distinct节点的消除。该规则用于消除IN/EXISTS子查询中的DISTINCT节点， 例如SELECT * FROM t1 WHERE EXISTS (SELECT
 * DISTINCT * FROM t2 WHERE t1.a = t2.a)，这里面的DISTINCT节点其实是不必要的。
 */
@AutoService(Rule.class)
public class InExistsDistinctEliminateRule extends Rule {

  public InExistsDistinctEliminateRule() {
    /*
     * we want to match the topology like:
     *             MarkJoin
     *           /         \
     *          ...        ...
     *                      |
     *                   Distinct
     *                      |
     *                     Any
     */
    super("InExistsDistinctEliminateRule", "DistinctEliminateRule", operand(Distinct.class, any()));
  }

  public boolean matches(RuleCall call) {
    // 向上找到MarkJoin节点，只有第一个Join节点是MarkJoin节点才能进行Distinct消除
    Distinct distinct = (Distinct) call.getMatchedRoot();
    Operator operator = findUpFirstJoin(call, distinct);
    return operator != null && operator.getType() == OperatorType.MarkJoin;
  }

  public void onMatch(RuleCall call) {
    // 把Distinct节点消除，替换为其子节点
    Distinct distinct = (Distinct) call.getMatchedRoot();
    call.transformTo(call.getChildrenIndex().get(distinct).get(0));
  }

  /**
   * 向上找到第一个Join节点
   *
   * @param operator 给定当前节点
   * @return 如果找到了，返回Join节点，否则返回null
   */
  private AbstractJoin findUpFirstJoin(RuleCall ruleCall, Operator operator) {
    if (operator == null) {
      return null;
    } else if (OperatorType.isJoinOperator(operator.getType())) {
      return (AbstractJoin) operator;
    } else {
      return findUpFirstJoin(ruleCall, ruleCall.getParentIndexMap().getOrDefault(operator, null));
    }
  }
}
