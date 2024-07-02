package cn.edu.tsinghua.iginx.engine.shared.expr;

public interface Expression {

  String getColumnName();

  ExpressionType getType();

  boolean hasAlias();

  String getAlias();

  void setAlias(String alias);

  void accept(ExpressionVisitor visitor);

  enum ExpressionType {
    Bracket,
    Binary,
    Unary,
    Function,
    Base,
    Constant,
    FromValue,
    Multiple,
  }
}
