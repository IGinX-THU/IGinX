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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ExprFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.List;
import java.util.logging.Logger;

public class FilterConstantFoldingRule extends Rule {
  private static final Logger LOGGER = Logger.getLogger(FilterConstantFoldingRule.class.getName());

  private static final class InstanceHolder {
    static final FilterConstantFoldingRule INSTANCE = new FilterConstantFoldingRule();
  }

  public static FilterConstantFoldingRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterConstantFoldingRule() {
    /*
     * we want to match the topology like:
     *         SELECT
     *           |
     *          Any
     */
    super("FilterConstantFoldingRule", operand(Select.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    // 获取 Select节点中的Expression Filter
    Select select = (Select) call.getMatchedRoot();
    Filter filter = select.getFilter();
    List<ExprFilter> exprFilterList = LogicalFilterUtils.getExprFilters(filter);

    // 遍历所有的ExprFilter
    for (ExprFilter exprFilter : exprFilterList) {
      // 如果ExprFilter中的表达式是常量表达式
      Expression flattenedExpressionA =
          ExprUtils.flattenExpression(ExprUtils.copy(exprFilter.getExpressionA()));
      Expression flattenedExpressionB =
          ExprUtils.flattenExpression(ExprUtils.copy(exprFilter.getExpressionB()));
      if (ExprUtils.hasMultiConstantsInMultipleExpression(flattenedExpressionA)
          || ExprUtils.hasMultiConstantsInMultipleExpression(flattenedExpressionB)) {
        // 如果拍平后的表达式中，存在一个multi节点，下面有多个常量，说明可以进行常量折叠优化
        return true;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    Filter filter = select.getFilter();
    List<ExprFilter> exprFilterList = LogicalFilterUtils.getExprFilters(filter);

    for (ExprFilter exprFilter : exprFilterList) {
      Expression flattenedExpressionA = ExprUtils.flattenExpression(exprFilter.getExpressionA());
      Expression flattenedExpressionB = ExprUtils.flattenExpression(exprFilter.getExpressionB());

      exprFilter.setExpressionA(ExprUtils.foldExpression(flattenedExpressionA));
      exprFilter.setExpressionB(ExprUtils.foldExpression(flattenedExpressionB));
    }
  }
}
