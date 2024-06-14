package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ConstantPropagationRule extends Rule {

  private static final Logger LOGGER = Logger.getLogger(ConstantPropagationRule.class.getName());

  private static final class InstanceHolder {
    static final ConstantPropagationRule INSTANCE = new ConstantPropagationRule();
  }

  public static ConstantPropagationRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected ConstantPropagationRule() {
    /*
     * we want to match the topology like:
     *         SELECT
     *           |
     *          Any
     */
    super("ConstantPropagationRule", operand(Select.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    // 检测是否有可传播的常量，即存在=条件，且同AND级别中存在可替换的变量
    Select select = (Select) call.getMatchedRoot();
    Filter filter = select.getFilter();
    Map<Filter, AndFilter> constantMap = getEqualValueFilter(filter);
    if (constantMap.isEmpty()) {
      return false;
    }

    // 接下来检测每个常量是否有可以传播的机会
    for (Map.Entry<Filter, AndFilter> entry : constantMap.entrySet()) {
      Filter constant = entry.getKey();
      AndFilter parent = entry.getValue();
      if (hasPathInAndFilter(constant, parent)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    Filter filter = select.getFilter();
    Map<Filter, AndFilter> constantMap = getEqualValueFilter(filter);
    for (Map.Entry<Filter, AndFilter> entry : constantMap.entrySet()) {
      Filter constant = entry.getKey();
      AndFilter parent = entry.getValue();
      try {
        propagateConstant(constant, parent);
      } catch (PhysicalException e) {
        throw new RuntimeException(e);
      }
    }

    select.setFilter(LogicalFilterUtils.mergeTrue(select.getFilter()));
  }

  /**
   * 检测给定的AND Filter是否包含指定的filter中的Path
   *
   * @param targetFilter Path对应的Filter，该Filter内的Path不算，要排除
   * @param parentAndFilter AND Filter，是指定Filter的父Filter
   * @return 是否包含指定的Path
   */
  private boolean hasPathInAndFilter(Filter targetFilter, AndFilter parentAndFilter) {
    String targetPath = getConstantPathFromFilter(targetFilter).getK();

    boolean[] hasPath = {false};
    parentAndFilter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {
            if (filter == targetFilter || !targetPath.equals(Constants.KEY)) {
              return;
            }
            hasPath[0] = true;
          }

          @Override
          public void visit(ValueFilter filter) {
            if (filter == targetFilter) {
              return;
            }

            if (filter.getPath().equals(targetPath)) {
              hasPath[0] = true;
            }
          }

          @Override
          public void visit(PathFilter filter) {
            if (filter.getPathA().equals(targetPath) || filter.getPathB().equals(targetPath)) {
              hasPath[0] = true;
            }
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            Expression exprA = filter.getExpressionA();
            Expression exprB = filter.getExpressionB();

            if (ExprUtils.getPathFromExpr(exprA).contains(targetPath)
                || ExprUtils.getPathFromExpr(exprB).contains(targetPath)) {
              hasPath[0] = true;
            }
          }
        });
    return hasPath[0];
  }

  private Pair<String, Value> getConstantPathFromFilter(Filter filter) {
    if (filter instanceof KeyFilter) {
      return new Pair<>(Constants.KEY, new Value(((KeyFilter) filter).getValue()));
    } else if (filter instanceof ValueFilter) {
      return new Pair<>(((ValueFilter) filter).getPath(), ((ValueFilter) filter).getValue());
    }
    throw new IllegalArgumentException("Filter is not KeyFilter or ValueFilter.");
  }

  /**
   * 获取Filter中的可传播的常量，以及如果他的父filter是AND的话，获取父filter
   *
   * @param filter 根filter
   * @return Map<可传播Filter, AND父Filter>，如果没有AND父Filter，则value为null
   */
  private Map<Filter, AndFilter> getEqualValueFilter(Filter filter) {
    Map<Filter, AndFilter> map = new HashMap<>();
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {
            for (Filter child : filter.getChildren()) {
              if (child instanceof ValueFilter && Op.isEqualOp(((ValueFilter) child).getOp())) {
                map.put(child, filter);
              } else if (child instanceof KeyFilter && Op.isEqualOp(((KeyFilter) child).getOp())) {
                map.put(child, filter);
              }
            }
          }

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {}

          @Override
          public void visit(PathFilter filter) {}

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {}
        });

    return map;
  }

  /**
   * 传播常量，给定一个Filter（对应了一个Path和对应的常量），将这个常量传播到replaceFilter中
   *
   * @param constantFilter 常量Filter
   * @param replaceFilter 需要传播的Filter，根调用时是常量Filter的父AndFilter，之后递归调用
   * @return 传播后的Filter
   */
  private Filter propagateConstant(Filter constantFilter, Filter replaceFilter)
      throws PhysicalException {
    if (replaceFilter == constantFilter) {
      return replaceFilter;
    }
    Pair<String, Value> constant = getConstantPathFromFilter(constantFilter);
    String constantPath = constant.getK();
    Value constantValue = constant.getV();

    switch (replaceFilter.getType()) {
      case And:
        AndFilter andFilter = (AndFilter) replaceFilter;
        List<Filter> andNewChildren = new ArrayList<>();
        for (Filter child : andFilter.getChildren()) {
          andNewChildren.add(propagateConstant(constantFilter, child));
        }
        andFilter.setChildren(andNewChildren);
        return andFilter;
      case Or:
        OrFilter orFilter = (OrFilter) replaceFilter;
        List<Filter> orNewChildren = new ArrayList<>();
        for (Filter child : orFilter.getChildren()) {
          orNewChildren.add(propagateConstant(constantFilter, child));
        }
        orFilter.setChildren(orNewChildren);
        return orFilter;
      case Not:
        NotFilter notFilter = (NotFilter) replaceFilter;
        notFilter.setChild(propagateConstant(constantFilter, notFilter.getChild()));
        return notFilter;
      case Path:
        PathFilter pathFilter = (PathFilter) replaceFilter;
        String pathA = pathFilter.getPathA();
        String pathB = pathFilter.getPathB();
        Op pathOp = pathFilter.getOp();
        if (pathA.equals(constantPath) && pathB.equals(constantPath)) {
          // 如果Path的两边都可以替换，那么根据Op直接返回true或false
          if (Op.op2Str(pathOp).contains("=")) {
            return new BoolFilter(true);
          } else {
            return new BoolFilter(false);
          }
        } else if (pathA.equals(constantPath)) {
          // 如果其中一边可以替换，构造一个新的ValueFilter
          return new ValueFilter(pathB, Op.getDirectionOpposite(pathOp), constantValue);
        } else if (pathB.equals(constantPath)) {
          return new ValueFilter(pathA, pathOp, constantValue);
        }
        return pathFilter;
      case Expr:
        ExprFilter exprFilter = (ExprFilter) replaceFilter;
        Expression exprA = exprFilter.getExpressionA();
        Expression exprB = exprFilter.getExpressionB();
        return new ExprFilter(
            replacePathInExpr(exprA, constantPath, constantValue),
            exprFilter.getOp(),
            replacePathInExpr(exprB, constantPath, constantValue));
      case Value:
        ValueFilter valueFilter = (ValueFilter) replaceFilter;
        Op valueOp = valueFilter.getOp();
        Value valueValue = valueFilter.getValue();
        if (valueFilter.getPath().equals(constantPath)) {
          return new BoolFilter(
              FilterUtils.validateValueCompare(valueOp, constantValue, valueValue));
        }
        return valueFilter;
      case Key:
        KeyFilter keyFilter = (KeyFilter) replaceFilter;
        Op keyOp = keyFilter.getOp();
        Value keyValue = new Value(keyFilter.getValue());
        if (constantPath.equals(Constants.KEY)) {
          return new BoolFilter(FilterUtils.validateValueCompare(keyOp, constantValue, keyValue));
        }
        return keyFilter;
      default:
        return replaceFilter;
    }
  }

  /**
   * 在表达式Expression中将指定的Path替换为Value
   *
   * @param expr 表达式
   * @param path 要替换的Path
   * @param value 替换的Value
   * @return 替换后的表达式
   */
  private Expression replacePathInExpr(Expression expr, String path, Value value) {
    switch (expr.getType()) {
      case Base:
        BaseExpression baseExpr = (BaseExpression) expr;
        if (baseExpr.getPathName().equals(path)) {
          return new ConstantExpression(value.getValue());
        }
        return baseExpr;
      case Binary:
        BinaryExpression binaryExpr = (BinaryExpression) expr;
        Expression exprA = binaryExpr.getLeftExpression();
        Expression exprB = binaryExpr.getRightExpression();
        return new BinaryExpression(
            replacePathInExpr(exprA, path, value),
            replacePathInExpr(exprB, path, value),
            binaryExpr.getOp());
      case Unary:
        UnaryExpression unaryExpr = (UnaryExpression) expr;
        return new UnaryExpression(
            unaryExpr.getOperator(), replacePathInExpr(unaryExpr.getExpression(), path, value));
      case Bracket:
        BracketExpression bracketExpr = (BracketExpression) expr;
        return new BracketExpression(replacePathInExpr(bracketExpr.getExpression(), path, value));
      default:
        return expr;
    }
  }
}
