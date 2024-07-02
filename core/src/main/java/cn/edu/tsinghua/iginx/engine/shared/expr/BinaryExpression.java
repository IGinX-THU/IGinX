/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
  public ExpressionType getType() {
    return ExpressionType.Binary;
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
    leftExpression.accept(visitor);
    rightExpression.accept(visitor);
  }
}
