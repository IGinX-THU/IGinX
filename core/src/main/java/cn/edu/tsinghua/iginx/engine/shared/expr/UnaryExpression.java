package cn.edu.tsinghua.iginx.engine.shared.expr;

public class UnaryExpression implements Expression {

  private final Operator operator;
  private Expression expression;
  private String alias;

  public UnaryExpression(Operator operator, Expression expression) {
    this(operator, expression, "");
  }

  public UnaryExpression(Operator operator, Expression expression, String alias) {
    this.operator = operator;
    this.expression = expression;
    this.alias = alias;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  @Override
  public String getColumnName() {
    return Operator.operatorToString(operator) + " " + expression.getColumnName();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Unary;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.equals("");
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
    expression.accept(visitor);
  }
}
