package cn.edu.tsinghua.iginx.sql.expression;

public interface Expression {

  String getColumnName();

  ExpressionType getType();

  boolean hasAlias();

  String getAlias();

  void setAlias(String alias);

  enum ExpressionType {
    Bracket,
    Binary,
    Unary,
    Function,
    Base,
    Constant,
    FromValue
  }
}
