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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NoExceptionAutoClosableHolder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TransformProjectionInfoGenerator
    implements UnaryExecutorInitializer<List<PhysicalExpression>> {

  private final RowTransform operator;

  public TransformProjectionInfoGenerator(RowTransform operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  public List<PhysicalExpression> initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<PhysicalExpression> temp = new ArrayList<>();
    try {
      if (inputSchema.hasKey()) {
        temp.add(new FieldNode(inputSchema.getKeyIndex(), BatchSchema.KEY.getName()));
      }
      for (int targetIndex = 0;
           targetIndex < operator.getFunctionCallList().size();
           targetIndex++) {
        FunctionCall functionCall = operator.getFunctionCallList().get(targetIndex);
        PhysicalExpression expression = getPhysicalExpression(context, inputSchema, functionCall);
        temp.add(expression);
      }
      List<PhysicalExpression> ret = new ArrayList<>(temp);
      temp.clear();
      return ret;
    } finally {
      temp.forEach(PhysicalExpression::close);
    }
  }

  private PhysicalExpression getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, FunctionCall functionCall)
      throws ComputeException {
    Function function = functionCall.getFunction();
    FunctionParams params = functionCall.getParams();
    switch (function.getFunctionType()) {
      case System:
        if (function.getMappingType() != MappingType.RowMapping) {
          throw new UnsupportedOperationException(
              "Unsupported mapping type for row transform: " + function.getMappingType());
        }
        if (function instanceof ArithmeticExpr) {
          Expression expr = params.getExpr();
          return getPhysicalExpression(context, inputSchema, expr);
        } else {
          throw new UnsupportedOperationException(
              "Unsupported system function: " + function.getIdentifier());
        }
      case UDF:
      default:
        throw new UnsupportedOperationException(
            "Unsupported function type: " + function.getFunctionType());
    }
  }

  private PhysicalExpression getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, Expression expr) throws ComputeException {
    switch (expr.getType()) {
      case Multiple:
      case FromValue:
        throw new IllegalArgumentException(String.format("%s not implemented", expr.getType()));
      case Base:
        return getPhysicalExpression(context, inputSchema, (BaseExpression) expr);
      case Binary:
        return getPhysicalExpression(context, inputSchema, (BinaryExpression) expr);
      case Unary:
        return getPhysicalExpression(context, inputSchema, (UnaryExpression) expr);
      case Bracket:
      case CaseWhen:
      case Constant:
      case Function:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private PhysicalExpression getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, BaseExpression expr) {
    Integer index = inputSchema.indexOf(expr.getColumnName());
    if (index == null) {
      return new LiteralNode(null);
    } else {
      return new FieldNode(index);
    }
  }

  private PhysicalExpression getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, BinaryExpression expr)
      throws ComputeException {
    try (NoExceptionAutoClosableHolder holder = new NoExceptionAutoClosableHolder()) {
      PhysicalExpression left =
          holder.add(getPhysicalExpression(context, inputSchema, expr.getLeftExpression()));
      PhysicalExpression right =
          holder.add(getPhysicalExpression(context, inputSchema, expr.getRightExpression()));

      ScalarFunction function = holder.add(getArithmeticFunction(expr.getOp()));
      CallNode callNode = holder.add(new CallNode(function, expr.getColumnName(), left, right));

      holder.detachAll();
      return callNode;
    }
  }

  private PhysicalExpression getPhysicalExpression(
      ExecutorContext context, BatchSchema inputSchema, UnaryExpression expr)
      throws ComputeException {
    PhysicalExpression expression = getPhysicalExpression(context, inputSchema, expr.getExpression());
    switch (expr.getOperator()) {
      case PLUS:
        return expression;
      case MINUS:
        return new CallNode(new Negate(), expr.getColumnName(), expression);
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
  }

  private BinaryArithmeticFunction getArithmeticFunction(Operator operator) {
    switch (operator) {
      case PLUS:
        return new Add();
      case MINUS:
        return new Minus();
      case STAR:
        return new Multiply();
      case DIV:
        return new Ratio();
      case MOD:
        return new Mod();
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
  }
}
