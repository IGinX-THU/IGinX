package cn.edu.tsinghua.iginx.engine.shared.expr;

public class BinaryExpression implements Expression {

  private Expression leftExpression;
  private Expression rightExpression;
  private Operator op;
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

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  public void setRightExpression(Expression rightExpression) {
    this.rightExpression = rightExpression;
  }

  public Operator getOp() {
    return op;
  }

  public void setOp(Operator op) {
    this.op = op;
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

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
    leftExpression.accept(visitor);
    rightExpression.accept(visitor);
  }
}
