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

import static cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer.TransformProjectionInfoGenerator.getPhysicalExpression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.compare.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.Not;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.Or;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.FilterUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.Generators;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.apache.arrow.vector.BitVector;

public class FilterInfoGenerator implements UnaryExecutorFactory<FilterUnaryExecutor> {

  private final Filter filter;

  public FilterInfoGenerator(Filter filter) {
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  public FilterUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    ScalarExpression<BitVector> condition = getCondition(context, inputSchema);
    List<FieldNode> outputExpressions = Generators.allFieldExpressions(inputSchema.getFieldCount());
    return new FilterUnaryExecutor(context, inputSchema, condition, outputExpressions);
  }

  public ScalarExpression<BitVector> getCondition(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    return construct(filter, context, inputSchema);
  }

  static ScalarExpression<BitVector> construct(
      Filter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    switch (filter.getType()) {
      case Value:
        return construct((ValueFilter) filter, context, inputSchema);
      case And:
        return construct((AndFilter) filter, context, inputSchema);
      case Or:
        return construct((OrFilter) filter, context, inputSchema);
      case Bool:
        return LiteralNode.of(((BoolFilter) filter).isTrue());
      case Key:
        return construct((KeyFilter) filter, context, inputSchema);
      case Not:
        return construct((NotFilter) filter, context, inputSchema);
      case Expr:
        return construct((ExprFilter) filter, context, inputSchema);
      case Path:
        return construct((PathFilter) filter, context, inputSchema);
      default:
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
    }
  }

  private static ScalarExpression<BitVector> construct(
      AndFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return and(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static ScalarExpression<BitVector> construct(
      OrFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return or(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static ScalarExpression<BitVector> construct(
      KeyFilter keyFilter, ExecutorContext context, BatchSchema inputSchema) {
    if (!inputSchema.hasKey()) {
      return LiteralNode.of(false);
    }
    return new CallNode<>(
        getPredicate(keyFilter.getOp()),
        new FieldNode(inputSchema.getKeyIndex()),
        new LiteralNode<>(keyFilter.getValue()));
  }

  private static ScalarExpression<BitVector> construct(
      NotFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return new CallNode<>(new Not(), construct(filter.getChild(), context, inputSchema));
  }

  private static ScalarExpression<BitVector> construct(
      ExprFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return new CallNode<>(
        getPredicate(filter.getOp()),
        getPhysicalExpression(context, inputSchema, filter.getExpressionA()),
        getPhysicalExpression(context, inputSchema, filter.getExpressionB()));
  }

  private static ScalarExpression<BitVector> construct(
      PathFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    if (inputSchema.indexOf(filter.getPathA()) == null
        || inputSchema.indexOf(filter.getPathB()) == null) {
      throw new ComputeException("Trying to compare non-existing path(s).");
    }
    return new CallNode<>(
        getPredicate(filter.getOp()),
        new FieldNode(inputSchema.indexOf(filter.getPathA())),
        new FieldNode(inputSchema.indexOf(filter.getPathB())));
  }

  private static List<ScalarExpression<BitVector>> construct(
      List<Filter> filters, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<BitVector>> result = new ArrayList<>();
    for (Filter filter : filters) {
      result.add(construct(filter, context, inputSchema));
    }
    return result;
  }

  private static ScalarExpression<BitVector> and(
      List<ScalarExpression<BitVector>> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(true), context, inputSchema);
    }
    return binaryReduce(children, (l, r) -> new CallNode<>(new And(), l, r), 0, children.size());
  }

  private static <T> T binaryReduce(
      List<T> list, BiFunction<T, T, T> combiner, int start, int end) {
    if (start == end) {
      throw new IllegalArgumentException("Empty list");
    }
    if (start == end - 1) {
      return list.get(start);
    }
    int mid = start + (end - start) / 2;
    T left = binaryReduce(list, combiner, start, mid);
    T right = binaryReduce(list, combiner, mid, end);
    return combiner.apply(left, right);
  }

  private static ScalarExpression<BitVector> or(
      List<ScalarExpression<BitVector>> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(false), context, inputSchema);
    }
    return binaryReduce(children, (l, r) -> new CallNode<>(new Or(), l, r), 0, children.size());
  }

  private static ScalarExpression<BitVector> construct(
      ValueFilter filter, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<Integer> paths = Schemas.matchPattern(inputSchema.raw(), filter.getPath());
    List<ScalarExpression<BitVector>> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      comparisons.add(
          new CallNode<>(
              getPredicate(filter.getOp()),
              new FieldNode(pathIndex),
              new LiteralNode<>(filter.getValue().getValue())));
    }
    if (Op.isOrOp(filter.getOp())) {
      return or(comparisons, context, inputSchema);
    } else if (Op.isAndOp(filter.getOp())) {
      return and(comparisons, context, inputSchema);
    } else {
      throw new UnsupportedOperationException("Unsupported operator: " + filter.getOp());
    }
  }

  private static ScalarFunction<BitVector> getPredicate(Op op) {
    switch (op) {
      case GE:
      case GE_AND:
        return new GreaterEqual();
      case LE:
      case LE_AND:
        return new LessEqual();
      case L:
      case L_AND:
        return new Less();
      case E:
      case E_AND:
        return new Equal();
      case G:
      case G_AND:
        return new Greater();
      case NE:
      case NE_AND:
        return new NotEqual();
      case LIKE:
      case LIKE_AND:
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }
}
