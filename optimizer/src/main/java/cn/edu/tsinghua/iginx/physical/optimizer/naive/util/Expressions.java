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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.Callee;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting.CaseWhen;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public class Expressions {

  public static AbstractScalarExpression<?> getPhysicalExpression(
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
        return getPhysicalExpression(context, inputSchema, (BracketExpression) expr, setAlias);
      case Constant:
        return getPhysicalExpression(context, inputSchema, (ConstantExpression) expr, setAlias);
      case CaseWhen:
        return getPhysicalExpression(context, inputSchema, (CaseWhenExpression) expr, setAlias);
      case Function:
      case Multiple:
        throw new IllegalArgumentException(String.format("%s not implemented", expr.getType()));
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private static AbstractScalarExpression<?> getPhysicalExpression(
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

  private static AbstractScalarExpression<?> getPhysicalExpression(
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

  private static AbstractScalarExpression<?> getPhysicalExpression(
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

  private static AbstractScalarExpression<?> getPhysicalExpression(
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

  private static AbstractScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, BracketExpression expr, boolean setAlias)
      throws ComputeException {
    AbstractScalarExpression<?> child =
        getPhysicalExpression(context, inputSchema, expr.getExpression(), false);
    if (setAlias) {
      return child.with(expr.getColumnName());
    } else {
      return child;
    }
  }

  private static AbstractScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, ConstantExpression expr, boolean setAlias) {
    if (setAlias) {
      return new LiteralNode<>(expr.getValue(), context.getConstantPool(), expr.getColumnName());
    } else {
      return new LiteralNode<>(expr.getValue(), context.getConstantPool());
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

  public static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, FunctionCall functionCall)
      throws ComputeException {
    Function function = functionCall.getFunction();
    if (function.getMappingType() != MappingType.RowMapping) {
      throw new UnsupportedOperationException(
          "Unsupported mapping type for row transform: " + function.getMappingType());
    }

    FunctionParams params = functionCall.getParams();
    List<ScalarExpression<?>> inputs =
        Expressions.getRowMappingFunctionArgumentExpressions(
            context,
            inputSchema,
            params,
            functionCall.isNeedPreRowTransform() || function instanceof ArithmeticExpr);

    switch (function.getFunctionType()) {
      case System:
        Callee callee = context.getCalleeRegistry().get(function.getIdentifier());
        if (callee == null) {
          throw new ComputeException("Function not found: " + function.getIdentifier());
        }
        return callee.call(context, inputSchema, params.getArgs(), params.getKwargs(), inputs);
      case UDF:
      default:
        throw new UnsupportedOperationException(
            "Unsupported function type: " + function.getFunctionType());
    }
  }

  public static List<ScalarExpression<?>> getRowMappingFunctionArgumentExpressions(
      ExecutorContext context,
      Schema inputSchema,
      FunctionParams params,
      boolean needPreRowTransform)
      throws ComputeException {
    List<ScalarExpression<?>> scalarExpressions = new ArrayList<>();
    if (!needPreRowTransform) {
      for (String path : params.getPaths()) {
        List<Integer> matchedIndexes = Schemas.matchPattern(inputSchema, path);
        if (matchedIndexes.isEmpty()) {
          throw new ComputeException("Column not found: " + path + " in " + inputSchema);
        } else if (matchedIndexes.size() > 1) {
          throw new ComputeException("Ambiguous column: " + path + " in " + inputSchema);
        }
        for (int index : matchedIndexes) {
          scalarExpressions.add(new FieldNode(index));
        }
      }
    } else {
      List<ScalarExpression<?>> preRowTransform = new ArrayList<>();
      for (Expression expression : params.getExpressions()) {
        preRowTransform.add(
            Expressions.getPhysicalExpression(context, inputSchema, expression, true));
      }
      Schema schema =
          ScalarExpressions.getOutputSchema(context.getAllocator(), preRowTransform, inputSchema);
      for (String path : params.getPaths()) {
        List<Integer> matchedIndexes = Schemas.matchPattern(schema, path);
        if (matchedIndexes.isEmpty()) {
          throw new ComputeException("Column not found: " + path + " in " + inputSchema);
        } else if (matchedIndexes.size() > 1) {
          throw new ComputeException("Ambiguous column: " + path + " in " + inputSchema);
        }
        for (int index : matchedIndexes) {
          scalarExpressions.add(preRowTransform.get(index));
        }
      }
    }
    return scalarExpressions;
  }
}
