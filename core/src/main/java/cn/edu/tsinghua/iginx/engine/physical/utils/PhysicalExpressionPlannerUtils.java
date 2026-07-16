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
package cn.edu.tsinghua.iginx.engine.physical.utils;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.BinaryArithmeticScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic.Negate;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting.CaseWhen;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public class PhysicalExpressionPlannerUtils {

  public static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, Expression expr, boolean setAlias)
      throws ComputeException {
    switch (expr.getType()) {
      case FromValue:
        throw new UnsupportedOperationException("Unsupported expr type: FromValue");
      case Base:
        return getPhysicalExpression(context, inputSchema, (BaseExpression) expr, setAlias);
      case Key:
        return getPhysicalExpression(context, inputSchema, (KeyExpression) expr, setAlias);
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
        return getPhysicalExpression(context, inputSchema, (FuncExpression) expr, setAlias);
      case Multiple:
        return getPhysicalExpression(context, inputSchema, (MultipleExpression) expr, setAlias);
      case Sequence:
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, BaseExpression expr, boolean setAlias)
      throws ComputeException {
    List<Integer> indexes = Schemas.matchPatternIgnoreKey(inputSchema, expr.getColumnName());
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
      ExecutorContext context, Schema inputSchema, KeyExpression expr, boolean setAlias)
      throws ComputeException {
    List<Integer> indexes = Schemas.matchPattern(inputSchema, BatchSchema.KEY.getName());
    if (indexes.isEmpty()) {
      throw new ComputeException("Key not found in " + inputSchema);
    }
    if (indexes.size() > 1) {
      throw new ComputeException("Ambiguous key in " + inputSchema);
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
      args.add(
          PhysicalFilterPlannerUtils.construct(expr.getConditions().get(i), context, inputSchema));
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

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, BracketExpression expr, boolean setAlias)
      throws ComputeException {
    ScalarExpression<?> child =
        getPhysicalExpression(context, inputSchema, expr.getExpression(), false);
    if (setAlias) {
      return new RenameNode<>(expr.getColumnName(), child);
    } else {
      return child;
    }
  }

  private static ScalarExpression<?> getPhysicalExpression(
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

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, FuncExpression expr, boolean setAlias)
      throws ComputeException {
    String columnName = expr.getColumnName();
    List<Integer> matchedIndexes = Schemas.matchPatternIgnoreKey(inputSchema, columnName);
    if (!matchedIndexes.isEmpty()) {
      if (matchedIndexes.size() > 1) {
        throw new ComputeException("Ambiguous column: " + columnName + " in " + inputSchema);
      }
      return new FieldNode(matchedIndexes.get(0));
    }

    if (expr.isDistinct()) {
      throw new ComputeException("Distinct is not supported in row mapping function expression");
    }
    String identifier = expr.getFuncName();
    FunctionParams params =
        new FunctionParams(expr.getExpressions(), expr.getArgs(), expr.getKvargs());

    ScalarExpression<?> physicalExpression =
        getPhysicalExpressionOfFunctionCall(context, inputSchema, identifier, params, setAlias);
    if (setAlias) {
      return new RenameNode<>(columnName, physicalExpression);
    } else {
      return physicalExpression;
    }
  }

  /**
   * transform multiple expressions to binary expr tree(balanced) bc arrow does not need flattened
   * expr for performance
   */
  private static Expression buildBiTreeFromMultiple(
      List<Expression> expressionList, List<Operator> operatorList, int start, int end) {
    if (start == end) {
      if (expressionList.get(start) instanceof MultipleExpression) {
        MultipleExpression multipleExpression = (MultipleExpression) expressionList.get(start);
        List<Expression> childExpressions = new ArrayList<>(multipleExpression.getChildren());
        List<Operator> childOperators = new ArrayList<>(multipleExpression.getOps());
        if (childOperators.get(0) != Operator.PLUS) {
          childExpressions.set(
              0, new UnaryExpression(childOperators.get(0), childExpressions.get(0)));
        }
        childOperators.remove(0);
        return buildBiTreeFromMultiple(
            childExpressions, childOperators, 0, childExpressions.size() - 1);
      } else {
        return expressionList.get(start);
      }
    }

    if (start + 1 == end) {
      return new BinaryExpression(
          buildBiTreeFromMultiple(expressionList, operatorList, start, start),
          buildBiTreeFromMultiple(expressionList, operatorList, end, end),
          operatorList.get(start));
    }

    int mid = (start + end + 1) / 2;
    return new BinaryExpression(
        buildBiTreeFromMultiple(expressionList, operatorList, start, mid),
        buildBiTreeFromMultiple(expressionList, operatorList, mid + 1, end),
        operatorList.get(mid));
  }

  private static ScalarExpression<?> getPhysicalExpression(
      ExecutorContext context, Schema inputSchema, MultipleExpression expr, boolean setAlias)
      throws ComputeException {
    List<Expression> expressionList = new ArrayList<>(expr.getChildren());
    List<Operator> operatorList = new ArrayList<>(expr.getOps());
    if (expressionList.isEmpty()) {
      // empty
      throw new ComputeException("Multiple expressions does not have any children");
    }
    if (expressionList.size() == 1) {
      // 1 element, 1st op is its sign
      UnaryExpression expression = new UnaryExpression(expr.getOpType(), expressionList.get(0));
      return getPhysicalExpression(context, inputSchema, expression, setAlias);
    }
    // >1 element, build tree
    if (operatorList.get(0) != Operator.PLUS) {
      // exclude PLUS to build right column name
      expressionList.set(0, new UnaryExpression(operatorList.get(0), expressionList.get(0)));
    }
    operatorList.remove(0);
    BinaryExpression expression =
        (BinaryExpression)
            buildBiTreeFromMultiple(expressionList, operatorList, 0, expressionList.size() - 1);
    return getPhysicalExpression(context, inputSchema, expression, setAlias);
  }

  public static ScalarExpression<?> getPhysicalExpressionOfFunctionCall(
      ExecutorContext context, Schema inputSchema, FunctionCall functionCall, boolean setAlias)
      throws ComputeException {
    Function function = functionCall.getFunction();
    if (function.getMappingType() != MappingType.RowMapping) {
      throw new UnsupportedOperationException(
          "Unsupported mapping type for row transform: " + function.getMappingType());
    }

    return ((RowMappingFunction) function)
        .transform(context, inputSchema, functionCall.getParams(), setAlias);
  }

  public static ScalarExpression<?> getPhysicalExpressionOfFunctionCall(
      ExecutorContext context,
      Schema inputSchema,
      String funcName,
      FunctionParams params,
      boolean setAlias)
      throws ComputeException {
    Function function = FunctionManager.getInstance().getFunction(funcName);
    return getPhysicalExpressionOfFunctionCall(
        context, inputSchema, new FunctionCall(function, params), setAlias);
  }

  public static List<ScalarExpression<?>> getRowMappingFunctionArgumentExpressions(
      ExecutorContext context, Schema inputSchema, FunctionParams params, boolean setAlias)
      throws ComputeException {
    List<ScalarExpression<?>> scalarExpressions = new ArrayList<>();
    boolean needPreRowTransform = false;
    for (Expression expression : params.getExpressions()) {
      if (!(expression instanceof BaseExpression)) {
        needPreRowTransform = true;
        break;
      }
    }
    if (!needPreRowTransform) {
      for (String path : params.getPaths()) {
        List<Integer> matchedIndexes = Schemas.matchPatternIgnoreKey(inputSchema, path);
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
            PhysicalExpressionPlannerUtils.getPhysicalExpression(
                context, inputSchema, expression, setAlias));
      }
      Schema schema =
          ScalarExpressionUtils.getOutputSchema(
              context.getAllocator(), preRowTransform, inputSchema);
      for (String path : params.getPaths()) {
        List<Integer> matchedIndexes = Schemas.matchPatternIgnoreKey(schema, path);
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
