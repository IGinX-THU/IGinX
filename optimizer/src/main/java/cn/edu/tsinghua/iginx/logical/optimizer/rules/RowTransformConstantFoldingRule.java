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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowTransformConstantFoldingRule extends Rule {

  private static final class InstanceHolder {
    static final RowTransformConstantFoldingRule INSTANCE = new RowTransformConstantFoldingRule();
  }

  public static RowTransformConstantFoldingRule getInstance() {
    return RowTransformConstantFoldingRule.InstanceHolder.INSTANCE;
  }

  protected RowTransformConstantFoldingRule() {
    /*
     * we want to match the topology like:
     *      RowTransform
     *           |
     *          Any
     */
    super("RowTransformConstantFoldingRule", operand(RowTransform.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    RowTransform rowTransform = (RowTransform) call.getMatchedRoot();
    List<FunctionCall> functionCallList = rowTransform.getFunctionCallList();
    for (FunctionCall functionCall : functionCallList) {
      Expression expr = functionCall.getParams().getExpr();
      if (expr != null) {
        Expression flattenedExpression = ExprUtils.flattenExpression(ExprUtils.copy(expr));
        if (ExprUtils.hasMultiConstantsInMultipleExpression(flattenedExpression)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void onMatch(RuleCall call) {
    RowTransform rowTransform = (RowTransform) call.getMatchedRoot();
    List<FunctionCall> functionCallList = rowTransform.getFunctionCallList();
    Map<String, String> aliasMap = new HashMap<>();

    for (FunctionCall functionCall : functionCallList) {
      Expression expr = functionCall.getParams().getExpr();
      if (expr != null) {
        String oldName = expr.getColumnName();
        Expression flattenedExpression = ExprUtils.flattenExpression(expr);
        if (ExprUtils.hasMultiConstantsInMultipleExpression(flattenedExpression)) {
          Expression foldedExpression = ExprUtils.foldExpression(flattenedExpression);
          functionCall.getParams().setExpr(foldedExpression);
          String newName = foldedExpression.getColumnName();
          aliasMap.put(newName, oldName);
        }
      }
    }

    // 改完之后要在上面加一层Rename，因为常量折叠改变了表达式输出的列名，但是我们依然要用原列名
    Rename rename = new Rename(new OperatorSource(rowTransform), aliasMap);
    call.transformTo(rename);
  }
}
