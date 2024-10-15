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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic.ArithmeticFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic.PhysicalMultiply;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NoExceptionAutoClosableHolder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Operator;
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
      case Base:
        return getPhysicalExpression(context, inputSchema, (BaseExpression) expr);
      case Binary:
        return getPhysicalExpression(context, inputSchema, (BinaryExpression) expr);
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

  public ArithmeticFunction getArithmeticFunction(Operator operator) {
    switch (operator) {
      case PLUS:
      case MINUS:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
      case STAR:
        return new PhysicalMultiply();
      case DIV:
      case MOD:
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
  }
}
