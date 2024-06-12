package cn.edu.tsinghua.iginx.engine.shared.expr;

public interface ExpressionVisitor {

  void visit(BaseExpression expression);

  void visit(BinaryExpression expression);

  void visit(BracketExpression expression);

  void visit(ConstantExpression expression);

  void visit(FromValueExpression expression);

  void visit(FuncExpression expression);

  void visit(MultipleExpression expression);

  void visit(UnaryExpression expression);
}
