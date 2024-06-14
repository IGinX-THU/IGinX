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
