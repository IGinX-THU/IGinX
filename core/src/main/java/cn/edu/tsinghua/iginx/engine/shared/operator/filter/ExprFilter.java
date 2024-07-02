package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;

public class ExprFilter implements Filter {

  private final FilterType type = FilterType.Expr;

  private Expression expressionA;

  private Expression expressionB;

  private Op op;

  public ExprFilter(Expression expressionA, Op op, Expression expressionB) {
    this.expressionA = expressionA;
    this.expressionB = expressionB;
    this.op = op;
  }

  public void reverseFunc() {
    this.op = Op.getOpposite(op);
  }

  public Expression getExpressionA() {
    return expressionA;
  }

  public void setExpressionA(Expression expressionA) {
    this.expressionA = expressionA;
  }

  public Expression getExpressionB() {
    return expressionB;
  }

  public void setExpressionB(Expression expressionB) {
    this.expressionB = expressionB;
  }

  public Op getOp() {
    return op;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public FilterType getType() {
    return type;
  }

  @Override
  public Filter copy() {
    return new ExprFilter(expressionA, op, expressionB);
  }

  @Override
  public String toString() {
    return expressionA.getColumnName() + " " + Op.op2Str(op) + " " + expressionB.getColumnName();
  }
}
