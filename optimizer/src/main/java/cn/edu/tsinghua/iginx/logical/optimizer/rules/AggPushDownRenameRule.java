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

import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.OptimizerUtils;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.auto.service.AutoService;
import java.util.List;

@AutoService(Rule.class)
public class AggPushDownRenameRule extends Rule {

  public AggPushDownRenameRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         Rename
     */
    super(
        AggPushDownRenameRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, operand(Rename.class, any())),
        2,
        RuleStrategy.FIXED_POINT);
  }

  @Override
  public boolean matches(RuleCall call) {
    return OptimizerUtils.validateAggPushDown(call.getMatchedRoot());
  }

  @Override
  public void onMatch(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    Rename rename = (Rename) call.getChildrenIndex().get(groupBy).get(0);
    groupBy
        .getGroupByExpressions()
        .replaceAll(expression -> recoverRenamedExpression(rename, expression));
    groupBy.updateGroupByCols();

    List<FunctionCall> functionCallList = groupBy.getFunctionCallList();
    for (FunctionCall functionCall : functionCallList) {
      String oldFuncStr = functionCall.getFunctionStr();
      functionCall
          .getParams()
          .getExpressions()
          .replaceAll(expression -> recoverRenamedExpression(rename, expression));
      functionCall.getParams().updatePaths();
      String newFuncStr = functionCall.getFunctionStr();
      if (!oldFuncStr.equals(newFuncStr)) {
        rename.getAliasList().add(new Pair<>(newFuncStr, oldFuncStr));
      }
    }

    groupBy.setSource(rename.getSource());
    rename.setSource(new OperatorSource(groupBy));
    call.transformTo(rename);
  }

  private Expression recoverRenamedExpression(Rename rename, Expression expression) {
    Expression copy = ExprUtils.copy(expression);
    copy.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            expression.setPathName(
                PathUtils.recoverRenamedPattern(rename.getAliasList(), expression.getPathName()));
          }

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {}

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {
            expression.setResultElse(recoverRenamedExpression(rename, expression.getResultElse()));
            expression.getResults().replaceAll(e -> recoverRenamedExpression(rename, e));
          }

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });

    return copy;
  }

  private void recoverRenamedFilter(Rename rename, Filter filter) {
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            filter.setPath(
                PathUtils.recoverRenamedPattern(rename.getAliasList(), filter.getPath()));
          }

          @Override
          public void visit(PathFilter filter) {
            filter.setPathA(
                PathUtils.recoverRenamedPattern(rename.getAliasList(), filter.getPathA()));
            filter.setPathB(
                PathUtils.recoverRenamedPattern(rename.getAliasList(), filter.getPathB()));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            filter.setExpressionA(recoverRenamedExpression(rename, filter.getExpressionA()));
            filter.setExpressionB(recoverRenamedExpression(rename, filter.getExpressionB()));
          }

          @Override
          public void visit(InFilter filter) {
            filter.setPath(
                PathUtils.recoverRenamedPattern(rename.getAliasList(), filter.getPath()));
          }
        });
  }
}
