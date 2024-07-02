package cn.edu.tsinghua.iginx.engine.shared.expr;

import java.util.List;

public class MultipleExpression implements Expression {
  // MultipleExpression只按顺序计算，不考虑优先级，所以运算符应该均为同优先级的运算符

  final List<Expression> children;

  final List<Operator> ops; // 每个子表达式之间的操作符，第一个操作符指的是第一个子表达式的正负号，其他是表达式之间的操作符

  Operator opType; // 该表达式的操作符类型，加减一类，乘除一类，模一类

  String alias;

  public MultipleExpression(List<Expression> children, List<Operator> ops, String alias) {
    this.children = children;
    this.ops = ops;
    this.alias = alias;
    if (ops.size() > 1) {
      opType = ops.get(1);
    } else {
      opType = null;
    }
  }

  public MultipleExpression(List<Expression> children, List<Operator> ops) {
    this(children, ops, null);
  }

  public List<Operator> getOps() {
    return ops;
  }

  public List<Expression> getChildren() {
    return children;
  }

  public void setChildren(List<Expression> children) {
    this.children.clear();
    this.children.addAll(children);
  }

  public void setOps(List<Operator> ops) {
    this.ops.clear();
    this.ops.addAll(ops);
  }

  public Operator getOpType() {
    return opType;
  }

  @Override
  public String getColumnName() {
    StringBuilder sb = new StringBuilder();
    if (ops.get(0) == Operator.MINUS) {
      sb.append("-");
    }
    for (int i = 0; i < children.size(); i++) {
      sb.append(children.get(i).getColumnName());
      if (i + 1 < ops.size()) {
        sb.append(" ");
        sb.append(Operator.operatorToString(ops.get(i + 1)));
        sb.append(" ");
      }
    }
    return sb.toString();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Multiple;
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
    children.forEach(e -> e.accept(visitor));
  }
}
