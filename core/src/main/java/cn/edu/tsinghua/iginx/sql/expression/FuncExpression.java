package cn.edu.tsinghua.iginx.sql.expression;

import java.util.List;

public class FuncExpression implements Expression {

  private final String funcName;
  private final List<String> params;
  private final boolean isDistinct;
  private String alias;

  public FuncExpression(String funcName, List<String> params) {
    this(funcName, params, "", false);
  }

  public FuncExpression(String funcName, List<String> params, boolean isDistinct) {
    this(funcName, params, "", isDistinct);
  }

  public FuncExpression(String funcName, List<String> params, String alias) {
    this(funcName, params, alias, false);
  }

  public FuncExpression(String funcName, List<String> params, String alias, boolean isDistinct) {
    this.funcName = funcName;
    this.params = params;
    this.alias = alias;
    this.isDistinct = isDistinct;
  }

  public String getFuncName() {
    return funcName;
  }

  public List<String> getParams() {
    return params;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public String getColumnName() {
    String columnName = funcName.toLowerCase() + "(";
    if (isDistinct) {
      columnName += "distinct ";
    }
    return columnName + String.join(", ", params) + ")";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Function;
  }

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
