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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.physical.optimizer.naive.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.BinaryArithmeticScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.Negate;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting.CaseWhen;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import java.util.ArrayList;
import java.util.List;

public class Expressions {

  public static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, Expression expr) throws ComputeException {
    switch (expr.getType()) {
      case FromValue:
        throw new UnsupportedOperationException("Unsupported expr type: FromValue");
      case Base:
        return getPhysicalExpression(context, inputSchema, (BaseExpression) expr);
      case Binary:
        return getPhysicalExpression(context, inputSchema, (BinaryExpression) expr);
      case Unary:
        return getPhysicalExpression(context, inputSchema, (UnaryExpression) expr);
      case Bracket:
        return getPhysicalExpression(
            context, inputSchema, ((BracketExpression) expr).getExpression());
      case Constant:
        return new LiteralNode<>(((ConstantExpression) expr).getValue(), context.getConstantPool());
      case CaseWhen:
        return getPhysicalExpression(context, inputSchema, (CaseWhenExpression) expr);
      case Function:
      case Multiple:
        throw new IllegalArgumentException(String.format("%s not implemented", expr.getType()));
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, BaseExpression expr)
      throws ComputeException {
    Integer index = inputSchema.indexOf(expr.getColumnName());
    if (index == null) {
      throw new ComputeException(
          "Column not found: " + expr.getColumnName() + " in " + inputSchema);
    } else {
      return new FieldNode(index);
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, CaseWhenExpression expr)
      throws ComputeException {
    List<ScalarExpression<?>> args = new ArrayList<>();
    // [condition1, value1, condition2, value2..., valueElse]
    for (int i = 0; i < expr.getConditions().size(); i++) {
      args.add(Filters.construct(expr.getConditions().get(i), context, inputSchema));
      args.add(getPhysicalExpression(context, inputSchema, expr.getResults().get(i)));
    }
    if (expr.getResultElse() != null) {
      args.add(getPhysicalExpression(context, inputSchema, expr.getResultElse()));
    }
    return new CallNode<>(new CaseWhen(), expr.getColumnName(), args);
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, BinaryExpression expr)
      throws ComputeException {
    ScalarExpression<?> left =
        getPhysicalExpression(context, inputSchema, expr.getLeftExpression());
    ScalarExpression<?> right =
        getPhysicalExpression(context, inputSchema, expr.getRightExpression());
    ScalarFunction<?> function = getArithmeticFunction(expr.getOp());
    return new CallNode<>(function, expr.getColumnName(), left, right);
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, UnaryExpression expr)
      throws ComputeException {
    ScalarExpression<?> expression =
        getPhysicalExpression(context, inputSchema, expr.getExpression());
    switch (expr.getOperator()) {
      case PLUS:
        return expression;
      case MINUS:
        return new CallNode<>(new Negate(), expr.getColumnName(), expression);
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + expr.getOperator());
    }
  }

  private static BinaryArithmeticScalarFunction getArithmeticFunction(Operator operator) {
    switch (operator) {
      case PLUS:
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic
            .Add();
      case MINUS:
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic
            .Minus();
      case STAR:
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic
            .Multiply();
      case DIV:
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic
            .Ratio();
      case MOD:
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic
            .Mod();
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
  }
}
