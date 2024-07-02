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
    // 如果是小数，保留小数点后5位
    if (value instanceof Double || value instanceof Float) {
      return String.format("%.5f", value);
    }
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

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
  }
}
