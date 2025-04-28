/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
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
  public void setColumnName(String databaseName) {
    for (Expression child : children) {
      child.setColumnName(databaseName);
    }
  }

  @Override
  public String getCalColumnName() {
    StringBuilder sb = new StringBuilder();
    if (ops.get(0) == Operator.MINUS) {
      sb.append("-");
    }
    for (int i = 0; i < children.size(); i++) {
      sb.append(children.get(i).getColumnName());
      if (i + 1 < ops.size()) {
        sb.append(" ");
        sb.append(Operator.operatorToCalString(ops.get(i + 1)));
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
    return alias != null && !alias.isEmpty();
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

  @Override
  public boolean equalExceptAlias(Expression expr) {
    if (this == expr) {
      return true;
    }
    if (expr == null || expr.getType() != ExpressionType.Multiple) {
      return false;
    }
    MultipleExpression that = (MultipleExpression) expr;

    if (this.getChildren().size() != that.getChildren().size()) {
      return false;
    }
    for (int i = 0; i < this.getChildren().size(); i++) {
      if (!this.getChildren().get(i).equalExceptAlias(that.getChildren().get(i))) {
        return false;
      }
    }

    return this.ops.equals(that.ops);
  }
}
