package cn.edu.tsinghua.iginx.engine.shared.expr;

public class BracketExpression implements Expression {

  private Expression expression;
  private String alias;

  public BracketExpression(Expression expression) {
    this(expression, "");
  }

  public BracketExpression(Expression expression, String alias) {
    this.expression = expression;
    this.alias = alias;
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  @Override
  public String getColumnName() {
    return "(" + expression.getColumnName() + ")";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Bracket;
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
