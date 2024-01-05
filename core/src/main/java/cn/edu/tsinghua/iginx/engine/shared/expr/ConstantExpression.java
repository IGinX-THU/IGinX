package cn.edu.tsinghua.iginx.engine.shared.expr;

public class ConstantExpression implements Expression {

  private final Object value;
  private String alias;

  public ConstantExpression(Object value) {
    this(value, "");
  }

  public ConstantExpression(Object value, String alias) {
    this.value = value;
    this.alias = alias;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String getColumnName() {
    return value.toString();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Constant;
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
