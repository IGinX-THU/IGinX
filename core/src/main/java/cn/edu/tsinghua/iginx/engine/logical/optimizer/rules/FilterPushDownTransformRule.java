package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.*;

public class FilterPushDownTransformRule extends Rule {

  static final Set<Class> validTransforms =
      new HashSet<>(Arrays.asList(MappingTransform.class, RowTransform.class, SetTransform.class));

  private static class InstanceHolder {
    private static final FilterPushDownTransformRule INSTANCE = new FilterPushDownTransformRule();
  }

  public FilterPushDownTransformRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownTransformRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *   Row/Set/MappingTransform
     */
    super(
        "FilterPushDownTransformRule", operand(Select.class, operand(AbstractUnaryOperator.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    if (!validTransforms.contains(operator.getClass())) {
      return false;
    }
    ;

    Filter filter = select.getFilter().copy();
    if (!(filter instanceof AndFilter)) {
      filter = LogicalFilterUtils.toCNF(filter);
    }
    AndFilter andFilter = (AndFilter) filter;
    List<FunctionCall> functionCallList = getFuncionCallList(operator);
    return andFilter.getChildren().stream()
        .anyMatch(child -> !filterHasFunction(child, functionCallList));
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    List<FunctionCall> functionCallList = getFuncionCallList(operator);

    Filter filter = select.getFilter().copy();
    if (!(filter instanceof AndFilter)) {
      filter = LogicalFilterUtils.toCNF(filter);
    }

    // 将Filter转换为CNF,带有Function的Filter不下推，不带Function的Filter下推
    AndFilter andFilter = (AndFilter) filter;
    List<Filter> remainFilters = new ArrayList<>(), pushDownFilters = new ArrayList<>();
    andFilter
        .getChildren()
        .forEach(
            child -> {
              if (filterHasFunction(child, functionCallList)) {
                remainFilters.add(child);
              } else {
                pushDownFilters.add(child);
              }
            });

    select.setFilter(new AndFilter(remainFilters));
    Select pushDownSelect =
        new Select(operator.getSource(), new AndFilter(pushDownFilters), select.getTagFilter());
    operator.setSource(new OperatorSource(pushDownSelect));
    call.transformTo(pushDownSelect);
  }

  private List<FunctionCall> getFuncionCallList(AbstractUnaryOperator operator) {
    if (operator instanceof MappingTransform) {
      return ((MappingTransform) operator).getFunctionCallList();
    } else if (operator instanceof RowTransform) {
      return ((RowTransform) operator).getFunctionCallList();
    } else if (operator instanceof SetTransform) {
      return ((SetTransform) operator).getFunctionCallList();
    } else {
      throw new IllegalArgumentException(
          "operator should be MappingTransform, RowTransform or SetTransform");
    }
  }

  private boolean filterHasFunction(Filter filter, List<FunctionCall> functionCallList) {
    final boolean[] hasFunction = {false};

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
            hasFunction[0] |=
                functionCallList.stream()
                    .anyMatch(
                        functionCall -> functionCall.getFunctionStr().equals(filter.getPath()));
          }

          @Override
          public void visit(PathFilter filter) {
            hasFunction[0] |=
                functionCallList.stream()
                    .anyMatch(
                        functionCall -> functionCall.getFunctionStr().equals(filter.getPathA()));
            hasFunction[0] |=
                functionCallList.stream()
                    .anyMatch(
                        functionCall -> functionCall.getFunctionStr().equals(filter.getPathB()));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            hasFunction[0] |= expressionHasFunction(filter.getExpressionA(), functionCallList);
            hasFunction[0] |= expressionHasFunction(filter.getExpressionB(), functionCallList);
          }
        });

    return hasFunction[0];
  }

  private boolean expressionHasFunction(
      Expression expression, List<FunctionCall> functionCallList) {
    final boolean[] hasFunction = {false};
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            hasFunction[0] |=
                functionCallList.stream()
                    .anyMatch(
                        functionCall ->
                            functionCall.getFunctionStr().equals(expression.getColumnName()));
          }

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {
            hasFunction[0] |=
                functionCallList.stream()
                    .anyMatch(
                        functionCall ->
                            functionCall.getFunctionStr().equals(expression.getColumnName()));
          }

          @Override
          public void visit(MultipleExpression expression) {}
        });

    return hasFunction[0];
  }
}
