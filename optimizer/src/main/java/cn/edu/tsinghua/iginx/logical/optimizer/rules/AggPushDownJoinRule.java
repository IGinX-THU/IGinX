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
import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Count;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.auto.service.AutoService;
import java.util.*;
import java.util.stream.Collectors;

@AutoService(Rule.class)
public class AggPushDownJoinRule extends Rule {
  public AggPushDownJoinRule() {
    /*
     * we want to match the topology like:
     *  GroupBy/Set Transform
     *           |
     *   Inner/Outer Join
     */
    super(AggPushDownJoinRule.class.getName(), "AggPushDownRule", any(),2, RuleStrategy.FIXED_POINT);
  }

  private static final Set<Operator> groupBySet = new HashSet<>();

  private static final Set<String> ALLOWED_FUNCTIONS =
      new HashSet<>(Arrays.asList("MAX", "MIN", "SUM", "COUNT"));

  @Override
  public boolean matches(RuleCall call) {
    Operator root = call.getMatchedRoot();

    if (!OptimizerUtils.validateAggPushDown(root)) {
      return false;
    }

    if (groupBySet.contains(root)) {
      return false;
    }

    Operator child = call.getChildrenIndex().get(root).get(0);
    if (child.getType() != OperatorType.OuterJoin && child.getType() != OperatorType.InnerJoin) {
      return false;
    }

    List<FunctionCall> functionCallList = OperatorUtils.getFunctionCallList(root);

    // 对于GroupBy有要求，仅当涉及的函数仅包含MAX, MIN, AVG, SUM, COUNT时才能下推
    if (!functionCallList.stream()
        .allMatch(
            fc -> ALLOWED_FUNCTIONS.contains(fc.getFunction().getIdentifier().toUpperCase()))) {
      return false;
    }

    // join中的条件,不能包含agg函数中涉及的列
    // 所有agg函数中的列必须都在左右某一侧（不能两侧都有）
    List<String> joinCondPaths = FilterUtils.getAllPathsFromFilter(getJoinCondition(child));
    String prefixA = getPrefixA(child);
    String prefixB = getPrefixB(child);
    for (FunctionCall fc : functionCallList) {
      List<String> funcPaths = ExprUtils.getPathFromExprList(fc.getParams().getExpressions());
      if (funcPaths.stream().anyMatch(joinCondPaths::contains)) {
        return false;
      } else if (funcPaths.stream().anyMatch(s -> s.startsWith(prefixA))
          && funcPaths.stream().anyMatch(s -> s.startsWith(prefixB))) {
        return false;
      }
    }

    // groupby下推过join是要超集下推的，因此如果下面的join过多的话，反而会减速,这点在tpc-h 2 11上有体现
    // 如果下方有超过2个join,就认为是不经济的

    final int[] joinCnt = {0};
    root.accept(new OperatorVisitor() {
      @Override
      public void visit(UnaryOperator unaryOperator) {
      }

      @Override
      public void visit(BinaryOperator binaryOperator) {
        if(binaryOperator.getType() == OperatorType.OuterJoin  || binaryOperator.getType() == OperatorType.InnerJoin){
          joinCnt[0]++;
        }
      }

      @Override
      public void visit(MultipleOperator multipleOperator) {

      }
    });

    if(joinCnt[0] > 2) return false;


    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    Operator root = call.getMatchedRoot();
    BinaryOperator child = (BinaryOperator) call.getChildrenIndex().get(root).get(0);

    groupBySet.add(root);

    List<String> groupByCols = new ArrayList<>();
    if (root.getType() == OperatorType.GroupBy) {
      GroupBy groupBy = (GroupBy) root;
      groupByCols = groupBy.getGroupByCols();
    }

    // root保持不变，超集下推groupby,groupby columns改为join条件的列
    List<FunctionCall> functionCallList = OperatorUtils.getFunctionCallList(root);
    List<FunctionCall> leftFunctionCallList = new ArrayList<>();
    List<FunctionCall> rightFunctionCallList = new ArrayList<>();

    List<String> leftPatterns =
        OperatorUtils.getPatternFromOperatorChildren(
            call.getChildrenIndex().get(child).get(0), new ArrayList<>());

    for (FunctionCall fc : functionCallList) {
      List<String> funcPaths = ExprUtils.getPathFromExprList(fc.getParams().getExpressions());

      if (funcPaths.stream().allMatch(s -> PathUtils.checkCoverage(leftPatterns, funcPaths))) {
        leftFunctionCallList.add(fc.copy());
      } else {
        rightFunctionCallList.add(fc.copy());
      }
    }

    List<String> joinCondPaths = FilterUtils.getAllPathsFromFilter(getJoinCondition(child));
    List<String> leftPatternsList =
        OperatorUtils.getPatternFromOperatorChildren(
            call.getChildrenIndex().get(child).get(0), new ArrayList<>());
    List<String> rightPatternsList =
        OperatorUtils.getPatternFromOperatorChildren(
            call.getChildrenIndex().get(child).get(1), new ArrayList<>());

    List<Expression> leftGroupByCols =
        joinCondPaths.stream()
            .filter(s -> leftPatternsList.stream().anyMatch(lp -> PathUtils.covers(lp, s)))
            .map(BaseExpression::new)
            .collect(Collectors.toList());
    List<Expression> rightGroupByCols =
        joinCondPaths.stream()
            .filter(s -> rightPatternsList.stream().anyMatch(rp -> PathUtils.covers(rp, s)))
            .map(BaseExpression::new)
            .collect(Collectors.toList());
    groupByCols.forEach(
        col -> {
          if (leftPatternsList.stream().anyMatch(lp -> PathUtils.covers(lp, col))
              && leftGroupByCols.stream().map(Expression::getColumnName).noneMatch(col::equals)) {
            leftGroupByCols.add(new BaseExpression(col));
          } else if (rightPatternsList.stream().anyMatch(rp -> PathUtils.covers(rp, col))
              && rightGroupByCols.stream().map(Expression::getColumnName).noneMatch(col::equals)) {
            rightGroupByCols.add(new BaseExpression(col));
          }
        });

    GroupBy leftGroupBy = new GroupBy(child.getSourceA(), leftGroupByCols, leftFunctionCallList);
    GroupBy rightGroupBy = new GroupBy(child.getSourceB(), rightGroupByCols, rightFunctionCallList);

    // 添加一个rename,将group by结果的函数形式列名改回原来的列名
    List<Pair<String, String>> leftRenameMap =
        leftFunctionCallList.stream()
            .map(fc -> new Pair<>(fc.getFunctionStr(), getRenamedParam(fc)))
            .collect(Collectors.toList());

    List<Pair<String, String>> rightRenameMap =
        rightFunctionCallList.stream()
            .map(fc -> new Pair<>(fc.getFunctionStr(), getRenamedParam(fc)))
            .collect(Collectors.toList());

    Rename leftRename = new Rename(new OperatorSource(leftGroupBy), leftRenameMap);
    Rename rightRename = new Rename(new OperatorSource(rightGroupBy), rightRenameMap);

    if (!leftGroupBy.getFunctionCallList().isEmpty()) {
      child.setSourceA(new OperatorSource(leftRename));
    }
    if (!rightGroupBy.getFunctionCallList().isEmpty()) {
      child.setSourceB(new OperatorSource(rightRename));
    }

    // 修改原Group by的函数参数为重命名后的结果
    List<Pair<String, String>> renameMap =
        functionCallList.stream()
            .map(fc -> new Pair<>("", fc.getFunctionStr()))
            .collect(Collectors.toList());

    for (int i = 0; i < functionCallList.size(); i++) {
      FunctionCall fc = functionCallList.get(i);
      fc.getParams().getExpressions().set(0, new BaseExpression(getRenamedParam(fc)));
      fc.getParams().updatePaths();
      // 如果是Count，需要修改函数
      if (fc.getFunction().getIdentifier().equalsIgnoreCase("COUNT")) {
        functionCallList.set(i, new FunctionCall(Count.getInstance(), fc.getParams()));
      }
    }

    for (int i = 0; i < renameMap.size(); i++) {
      renameMap.get(i).k = functionCallList.get(i).getFunctionStr();
    }
    Rename rename = new Rename(new OperatorSource(root), renameMap);

    call.transformTo(rename);
  }

  private Filter getJoinCondition(Operator op) {
    if (op.getType() == OperatorType.InnerJoin) {
      InnerJoin ij = (InnerJoin) op;
      return ij.getFilter();
    } else if (op.getType() == OperatorType.OuterJoin) {
      OuterJoin oj = (OuterJoin) op;
      return oj.getFilter();
    }

    return new BoolFilter(true);
  }

  private String getPrefixA(Operator op) {
    if (op.getType() == OperatorType.InnerJoin) {
      InnerJoin ij = (InnerJoin) op;
      return ij.getPrefixA();
    } else if (op.getType() == OperatorType.OuterJoin) {
      OuterJoin oj = (OuterJoin) op;
      return oj.getPrefixA();
    }

    return "";
  }

  private String getPrefixB(Operator op) {
    if (op.getType() == OperatorType.InnerJoin) {
      InnerJoin ij = (InnerJoin) op;
      return ij.getPrefixB();
    } else if (op.getType() == OperatorType.OuterJoin) {
      OuterJoin oj = (OuterJoin) op;
      return oj.getPrefixB();
    }

    return "";
  }

  public static String getRenamedParam(FunctionCall fc) {
    return fc.getFunction().getIdentifier()
        + "_"
        + fc.getParams().getPaths().get(0).replace(".", "_");
  }
}
