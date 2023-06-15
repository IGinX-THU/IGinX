package cn.edu.tsinghua.iginx.sql.expression;

public class BracketExpression implements Expression {

  private final Expression expression;
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
}
