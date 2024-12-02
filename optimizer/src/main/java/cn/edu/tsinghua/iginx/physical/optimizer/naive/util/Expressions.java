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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.BinaryArithmeticScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.Negate;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting.CaseWhen;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public class Expressions {

  public static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, Expression expr, boolean setAlias)
      throws ComputeException {
    switch (expr.getType()) {
      case FromValue:
        throw new UnsupportedOperationException("Unsupported expr type: FromValue");
      case Base:
        return getPhysicalExpression(context, inputSchema, (BaseExpression) expr, setAlias);
      case Binary:
        return getPhysicalExpression(context, inputSchema, (BinaryExpression) expr, setAlias);
      case Unary:
        return getPhysicalExpression(context, inputSchema, (UnaryExpression) expr, setAlias);
      case Bracket:
        return getPhysicalExpression(
            context, inputSchema, ((BracketExpression) expr).getExpression(), setAlias);
      case Constant:
        if (setAlias) {
          return new LiteralNode<>(
              ((ConstantExpression) expr).getValue(),
              context.getConstantPool(),
              expr.getColumnName());
        } else {
          return new LiteralNode<>(
              ((ConstantExpression) expr).getValue(), context.getConstantPool());
        }
      case CaseWhen:
        return getPhysicalExpression(context, inputSchema, (CaseWhenExpression) expr, setAlias);
      case Function:
      case Multiple:
        throw new IllegalArgumentException(String.format("%s not implemented", expr.getType()));
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, BaseExpression expr, boolean setAlias)
      throws ComputeException {
    List<Integer> indexes = Schemas.matchPattern(inputSchema, expr.getColumnName());
    if (indexes.isEmpty()) {
      throw new ComputeException(
          "Column not found: " + expr.getColumnName() + " in " + inputSchema);
    }
    if (indexes.size() > 1) {
      throw new ComputeException(
          "Ambiguous column: " + expr.getColumnName() + " in " + inputSchema);
    }
    if (setAlias) {
      return new FieldNode(indexes.get(0), expr.getColumnName());
    } else {
      return new FieldNode(indexes.get(0));
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, CaseWhenExpression expr, boolean setAlias)
      throws ComputeException {
    List<ScalarExpression<?>> args = new ArrayList<>();
    // [condition1, value1, condition2, value2..., valueElse]
    for (int i = 0; i < expr.getConditions().size(); i++) {
      args.add(Filters.construct(expr.getConditions().get(i), context, inputSchema));
      args.add(getPhysicalExpression(context, inputSchema, expr.getResults().get(i), false));
    }
    if (expr.getResultElse() != null) {
      args.add(getPhysicalExpression(context, inputSchema, expr.getResultElse(), false));
    }
    if (setAlias) {
      return new CallNode<>(new CaseWhen(), expr.getColumnName(), args);
    } else {
      return new CallNode<>(new CaseWhen(), args);
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, BinaryExpression expr, boolean setAlias)
      throws ComputeException {
    ScalarExpression<?> left =
        getPhysicalExpression(context, inputSchema, expr.getLeftExpression(), false);
    ScalarExpression<?> right =
        getPhysicalExpression(context, inputSchema, expr.getRightExpression(), false);
    ScalarFunction<?> function = getArithmeticFunction(expr.getOp());
    if (setAlias) {
      return new CallNode<>(function, expr.getColumnName(), left, right);
    } else {
      return new CallNode<>(function, left, right);
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, UnaryExpression expr, boolean setAlias)
      throws ComputeException {
    switch (expr.getOperator()) {
      case PLUS:
        return getPhysicalExpression(context, inputSchema, expr.getExpression(), true);
      case MINUS:
        return new CallNode<>(
            new Negate(),
            expr.getColumnName(),
            getPhysicalExpression(context, inputSchema, expr.getExpression(), false));
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
