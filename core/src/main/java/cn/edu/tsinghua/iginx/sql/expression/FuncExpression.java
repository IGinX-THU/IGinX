package cn.edu.tsinghua.iginx.sql.expression;

import java.util.List;

public class FuncExpression implements Expression {

  private final String funcName;
  private final List<String> params;
  private String alias;

  public FuncExpression(String funcName, List<String> params) {
    this(funcName, params, "");
  }

  public FuncExpression(String funcName, List<String> params, String alias) {
    this.funcName = funcName;
    this.params = params;
    this.alias = alias;
  }

  public String getFuncName() {
    return funcName;
  }

  public List<String> getParams() {
    return params;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public String getColumnName() {
    return funcName.toLowerCase() + "(" + String.join(", ", params) + ")";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Function;
  }

  public boolean hasAlias() {
    return alias != null && !alias.equals("");
  }
}
