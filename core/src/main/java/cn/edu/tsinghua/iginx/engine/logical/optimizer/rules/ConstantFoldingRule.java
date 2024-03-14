package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ExprFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import javassist.expr.Expr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ConstantFoldingRule extends Rule {
  private static final Logger LOGGER = Logger.getLogger(ConstantFoldingRule.class.getName());

  private static final Map<Operator, List<ExprFilter>> operatorExprFilterMap = new HashMap<>();

  private static final class InstanceHolder {
    static final ConstantFoldingRule INSTANCE = new ConstantFoldingRule();
  }

  public static ConstantFoldingRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected ConstantFoldingRule() {
    /*
     * we want to match the topology like:
     *         SELECT
     *           |
     *          Any
     */
    super("ConstantFoldingRule", operand(Select.class, any()));
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
      Expression flattenedExpressionA = ExprUtils.flattenExpression(exprFilter.getExpressionA());
      Expression flattenedExpressionB = ExprUtils.flattenExpression(exprFilter.getExpressionB());
      if (hasMultiConstantsInMultipleExpression(flattenedExpressionA)
          || hasMultiConstantsInMultipleExpression(flattenedExpressionB)) {
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

      exprFilter.setExpressionA(foldExpression(flattenedExpressionA));
      exprFilter.setExpressionB(foldExpression(flattenedExpressionB));
    }
  }

  private static Expression foldExpression(Expression expression){
    switch (expression.getType()){
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        multipleExpression.getChildren().replaceAll(ConstantFoldingRule::foldExpression);
        return ExprUtils.foldMultipleExpression(multipleExpression);
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        unaryExpression.setExpression(foldExpression(unaryExpression.getExpression()));
        return unaryExpression;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        binaryExpression.setLeftExpression(foldExpression(binaryExpression.getLeftExpression()));
        binaryExpression.setRightExpression(foldExpression(binaryExpression.getRightExpression()));
        return binaryExpression;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expression;
        bracketExpression.setExpression(foldExpression(bracketExpression.getExpression()));
        return bracketExpression;
      default:
        return expression;
    }

  }

  /**
   * 判断给定的Expression中是否有一个MultipleExpression，且其中包含多个常量表达式
   * @param expression 给定的Expression
   * @return 如果给定的Expression中有一个MultipleExpression，且其中包含多个常量表达式，则返回true，否则返回false
   */
  private static boolean hasMultiConstantsInMultipleExpression(Expression expression) {
    switch(expression.getType()){
      case Multiple:
          MultipleExpression multipleExpression = (MultipleExpression) expression;
          List<Expression> children = multipleExpression.getChildren();
          int constantCount = 0;
          for (Expression child : children) {
            if(child.getType() == Expression.ExpressionType.Constant){
              constantCount++;
              if(constantCount > 1){
                return true;
              }
            }else if (hasMultiConstantsInMultipleExpression(child)) {
                return true;
            }
          }
          return false;
      case Binary:
          return hasMultiConstantsInMultipleExpression(((BinaryExpression) expression).getLeftExpression()) ||
              hasMultiConstantsInMultipleExpression(((BinaryExpression) expression).getRightExpression());
      case Unary:
          return hasMultiConstantsInMultipleExpression(((UnaryExpression) expression).getExpression());
      case Bracket:
          return hasMultiConstantsInMultipleExpression(((BracketExpression) expression).getExpression());
      default:
          return false;
    }
  }


}
