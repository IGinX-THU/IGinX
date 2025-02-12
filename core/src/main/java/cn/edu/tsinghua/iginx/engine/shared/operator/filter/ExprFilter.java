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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import java.util.Objects;

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
    return new ExprFilter(ExprUtils.copy(expressionA), op, ExprUtils.copy(expressionB));
  }

  @Override
  public String toString() {
    return expressionA.getColumnName() + " " + Op.op2Str(op) + " " + expressionB.getColumnName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExprFilter that = (ExprFilter) o;
    return expressionA.equalExceptAlias(that.expressionA)
        && op == that.op
        && expressionB.equalExceptAlias(that.expressionB);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, expressionA, expressionB, op);
  }
}
