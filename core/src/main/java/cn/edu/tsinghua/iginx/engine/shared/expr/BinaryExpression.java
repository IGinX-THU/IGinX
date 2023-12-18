package cn.edu.tsinghua.iginx.engine.shared.expr;

public class BinaryExpression implements Expression {

  private final Expression leftExpression;
  private final Expression rightExpression;
  private final Operator op;
  private String alias;

  public BinaryExpression(Expression leftExpression, Expression rightExpression, Operator op) {
    this(leftExpression, rightExpression, op, "");
  }

  public BinaryExpression(
      Expression leftExpression, Expression rightExpression, Operator op, String alias) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
    this.op = op;
    this.alias = alias;
  }

  public Expression getLeftExpression() {
    return leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  public Operator getOp() {
    return op;
  }

  @Override
  public String getColumnName() {
    return leftExpression.getColumnName()
        + " "
        + Operator.operatorToString(op)
        + " "
        + rightExpression.getColumnName();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Binary;
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
}
