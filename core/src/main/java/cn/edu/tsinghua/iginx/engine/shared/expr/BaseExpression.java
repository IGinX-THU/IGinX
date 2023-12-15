package cn.edu.tsinghua.iginx.engine.shared.expr;

public class BaseExpression implements Expression {

  private final String pathName;
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
}
