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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Sum;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.auto.service.AutoService;
import java.util.*;
import java.util.stream.Collectors;

@AutoService(Rule.class)
public class AggPushDownUnionRule extends Rule {

  public AggPushDownUnionRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         Union
     */
    super(
        AggPushDownUnionRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, operand(Union.class, any(), any())));
  }

  private static final Set<String> ALLOWED_FUNCTIONS =
      new HashSet<>(Arrays.asList("MAX", "MIN", "SUM", "COUNT"));

  private static final Set<GroupBy> groupBySet = new HashSet<>();

  @Override
  public boolean matches(RuleCall call) {
    // 对于GroupBy有要求，仅当涉及的函数仅包含MAX, MIN, SUM, COUNT时才能下推
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    if (!OptimizerUtils.validateAggPushDown(groupBy)) {
      return false;
    }
    if (!(!groupBySet.contains(groupBy)
        && groupBy.getFunctionCallList().stream()
            .allMatch(
                fc ->
                    ALLOWED_FUNCTIONS.contains(fc.getFunction().getIdentifier().toUpperCase())))) {
      return false;
    }

    // 如果两侧表头有*，无法推测对应关系，也不能下推
    Union union = (Union) call.getChildrenIndex().get(groupBy).get(0);
    return union.getLeftOrder().stream().noneMatch(s -> s.contains("*"))
        && union.getRightOrder().stream().noneMatch(s -> s.contains("*"));
  }

  @Override
  public void onMatch(RuleCall call) {
    // 这里只能超级下推，Union上面还保留原GroupBy,但将子GroupBy下推到下面去
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    groupBySet.add(groupBy);
    Union union = (Union) call.getChildrenIndex().get(groupBy).get(0);

    List<String> leftOrder = union.getLeftOrder();
    List<String> rightOrder = union.getRightOrder();

    Map<String, String> left2Right = new HashMap<>();
    for (int i = 0; i < leftOrder.size(); i++) {
      left2Right.put(leftOrder.get(i), rightOrder.get(i));
    }

    // Union的左侧是作为Union结果表头的，可以直接下推，下推后需要相应更改Union信息,左侧的下推Group By结果需要rename
    GroupBy leftGroupBy = (GroupBy) groupBy.copy();
    leftGroupBy.getFunctionCallList().replaceAll(FunctionCall::copy);
    leftGroupBy.setSource(union.getSourceA());
    union.setSourceA(new OperatorSource(leftGroupBy));
    leftOrder.clear();
    leftOrder.addAll(groupBy.getGroupByCols());
    leftOrder.addAll(
        groupBy.getFunctionCallList().stream()
            .map(FunctionCall::getFunctionStr)
            .collect(Collectors.toList()));

    // Union的右侧的表头需要转换称左侧的表头，因此下推的GroupBy也要做相应转换
    List<Expression> groupByExpressions = new ArrayList<>();
    for (Expression expression : leftGroupBy.getGroupByExpressions()) {
      groupByExpressions.add(ExprUtils.replacePaths(expression, left2Right));
    }
    List<FunctionCall> functionCalls = new ArrayList<>();
    for (FunctionCall fc : leftGroupBy.getFunctionCallList()) {
      FunctionCall newFc = fc.copy();
      newFc.getParams().getExpressions().replaceAll(e -> ExprUtils.replacePaths(e, left2Right));
      newFc.getParams().updatePaths();
      functionCalls.add(newFc);
    }
    GroupBy rightGroupBy = new GroupBy(union.getSourceB(), groupByExpressions, functionCalls);
    union.setSourceB(new OperatorSource(rightGroupBy));

    rightOrder.clear();
    rightOrder.addAll(rightGroupBy.getGroupByCols());
    rightOrder.addAll(
        rightGroupBy.getFunctionCallList().stream()
            .map(FunctionCall::getFunctionStr)
            .collect(Collectors.toList()));

    // 最后要更改最上面原GroupBy的信息，function的param要更改成聚合过一遍的结果
    List<Pair<String, String>> renameMap = new ArrayList<>();

    for (int i = 0; i < groupBy.getFunctionCallList().size(); i++) {
      FunctionCall fc = groupBy.getFunctionCallList().get(i);
      String oldFuncStr = fc.getFunctionStr();
      fc.getParams().setExpression(0, new BaseExpression(fc.getFunctionStr()));
      fc.getParams().updatePaths();
      String newFuncStr = fc.getFunctionStr();
      renameMap.add(new Pair<>(newFuncStr, oldFuncStr));
      if (fc.getFunction().getIdentifier().equalsIgnoreCase("COUNT")) {
        groupBy.getFunctionCallList().set(i, new FunctionCall(Sum.getInstance(), fc.getParams()));
      }
    }

    Rename rename = new Rename(new OperatorSource(groupBy), renameMap);

    call.transformTo(rename);
  }
}
