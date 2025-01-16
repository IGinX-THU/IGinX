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

public class BinaryExpression implements Expression {

  private Expression leftExpression;
  private Expression rightExpression;
  private Operator op;
  private String alias;

  public BinaryExpression(Expression leftExpression, Expression rightExpression, Operator op) {
    this(leftExpression, rightExpression, op, "");
  }

  public BinaryExpression(
      Expression leftExpression, Expression rightExpression, Operator op, String alias) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
    this.op = op;
    this.alias = alias;
  }

  public Expression getLeftExpression() {
    return leftExpression;
  }

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  public void setRightExpression(Expression rightExpression) {
    this.rightExpression = rightExpression;
  }

  public Operator getOp() {
    return op;
  }

  public void setOp(Operator op) {
    this.op = op;
  }

  @Override
  public String getColumnName() {
    return leftExpression.getColumnName()
        + " "
        + Operator.operatorToString(op)
        + " "
        + rightExpression.getColumnName();
  }

  @Override
  public String getCalColumnName() {
    return leftExpression.getColumnName()
        + " "
        + Operator.operatorToCalString(op)
        + " "
        + rightExpression.getColumnName();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Binary;
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
    leftExpression.accept(visitor);
    rightExpression.accept(visitor);
  }

  @Override
  public boolean equalExceptAlias(Expression expr) {
    if (this == expr) {
      return true;
    }
    if (expr == null || expr.getType() != ExpressionType.Binary) {
      return false;
    }
    BinaryExpression that = (BinaryExpression) expr;
    return this.leftExpression.equalExceptAlias(that.leftExpression)
        && this.rightExpression.equalExceptAlias(that.rightExpression)
        && this.op == that.op;
  }
}
