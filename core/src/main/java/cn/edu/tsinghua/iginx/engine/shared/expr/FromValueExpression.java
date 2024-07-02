package cn.edu.tsinghua.iginx.engine.shared.expr;

import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class FromValueExpression implements Expression {

  private final SelectStatement subStatement;

  public FromValueExpression(SelectStatement subStatement) {
    this.subStatement = subStatement;
  }

  public SelectStatement getSubStatement() {
    return subStatement;
  }

  @Override
  public String getColumnName() {
    return "";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.FromValue;
  }

  @Override
  public boolean hasAlias() {
    return false;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public void setAlias(String alias) {}

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
  }
}
