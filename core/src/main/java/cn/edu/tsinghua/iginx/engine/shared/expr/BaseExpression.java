package cn.edu.tsinghua.iginx.engine.shared.expr;

public class BaseExpression implements Expression {

  private String pathName;
  private String alias;

  public BaseExpression(String pathName) {
    this(pathName, "");
  }

  public BaseExpression(String pathName, String alias) {
    this.pathName = pathName;
    this.alias = alias;
  }

  public String getPathName() {
    return pathName;
  }

  @Override
  public String getColumnName() {
    return pathName;
  }

  public void setPathName(String pathName) {
    this.pathName = pathName;
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Base;
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
