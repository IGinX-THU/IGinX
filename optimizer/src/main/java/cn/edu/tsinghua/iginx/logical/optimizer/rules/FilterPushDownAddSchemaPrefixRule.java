package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

public class FilterPushDownAddSchemaPrefixRule extends Rule {

  private static class InstanceHolder {
    private static final FilterPushDownAddSchemaPrefixRule INSTANCE =
        new FilterPushDownAddSchemaPrefixRule();
  }

  public static FilterPushDownAddSchemaPrefixRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownAddSchemaPrefixRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *     AddSchemaPrefix
     */
    super(
        "FilterPushDownAddSchemaPrefixRule",
        operand(Select.class, operand(AddSchemaPrefix.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AddSchemaPrefix addSchemaPrefix =
        (AddSchemaPrefix) ((OperatorSource) select.getSource()).getOperator();
    removeFilterPrefix(select.getFilter(), addSchemaPrefix.getSchemaPrefix());

    select.setSource(addSchemaPrefix.getSource());
    addSchemaPrefix.setSource(new OperatorSource(select));
    call.transformTo(addSchemaPrefix);
  }

  private void removeFilterPrefix(Filter filter, String prefix) {
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
            filter.setPath(removePrefix(filter.getPath(), prefix));
          }

          @Override
          public void visit(PathFilter filter) {
            filter.setPathA(removePrefix(filter.getPathA(), prefix));
            filter.setPathB(removePrefix(filter.getPathB(), prefix));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            Expression exprA = filter.getExpressionA();
            Expression exprB = filter.getExpressionB();
            removeExpressionPrefix(exprA, prefix);
            removeExpressionPrefix(exprB, prefix);
          }
        });
  }

  private void removeExpressionPrefix(Expression expression, String prefix) {
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            expression.setPathName(removePrefix(expression.getPathName(), prefix));
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
          public void visit(FuncExpression expression) {
            expression.getColumns().replaceAll(column -> removePrefix(column, prefix));
          }

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}
        });
  }

  private String removePrefix(String path, String prefix) {
    if (prefix != null && path.startsWith(prefix + ".")) {
      return path.substring(prefix.length() + 1);
    }
    return path;
  }
}
