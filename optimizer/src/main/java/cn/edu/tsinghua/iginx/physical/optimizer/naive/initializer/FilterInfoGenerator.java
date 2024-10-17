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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.GreaterEqual;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.Less;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.LessEqual;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.Or;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class FilterInfoGenerator implements UnaryExecutorInitializer<PhysicalExpression> {

  private final Filter filter;

  public FilterInfoGenerator(Filter filter) {
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  public PhysicalExpression initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    return construct(filter, context, inputSchema);
  }

  private static PhysicalExpression construct(
      Filter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    switch (filter.getType()) {
      case Value:
        return construct((ValueFilter) filter, context, inputSchema);
      case And:
        return construct((AndFilter) filter, context, inputSchema);
      case Or:
        return construct((OrFilter) filter, context, inputSchema);
      default:
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
    }
  }

  private static PhysicalExpression construct(
      AndFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return and(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static PhysicalExpression construct(
      OrFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return or(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static List<PhysicalExpression> construct(
      List<Filter> filters, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<PhysicalExpression> result = new ArrayList<>();
    for (Filter filter : filters) {
      result.add(construct(filter, context, inputSchema));
    }
    return result;
  }

  private static PhysicalExpression and(
      List<PhysicalExpression> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(true), context, inputSchema);
    }
    return binaryReduce(children, (l, r) -> new CallNode(new And(), l, r), 0, children.size());
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

  private static PhysicalExpression or(
      List<PhysicalExpression> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(false), context, inputSchema);
    }
    return binaryReduce(children, (l, r) -> new CallNode(new Or(), l, r), 0, children.size());
  }

  private static PhysicalExpression construct(
      ValueFilter filter, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<Integer> paths = match(filter.getPath(), context, inputSchema);
    List<PhysicalExpression> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      comparisons.add(
          new CallNode(
              getPredicate(filter.getOp()),
              new FieldNode(pathIndex),
              new LiteralNode(filter.getValue().getValue())));
    }
    if (Op.isOrOp(filter.getOp())) {
      return or(comparisons, context, inputSchema);
    } else if (Op.isAndOp(filter.getOp())) {
      return and(comparisons, context, inputSchema);
    } else {
      throw new UnsupportedOperationException("Unsupported operator: " + filter.getOp());
    }
  }

  private static ScalarFunction getPredicate(Op op) {
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
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }

  private static List<Integer> match(
      String path, ExecutorContext context, BatchSchema inputSchema) {
    Predicate<String> matcher = StringUtils.toColumnMatcher(path);
    List<Integer> result = new ArrayList<>();
    for (int i = 0; i < inputSchema.getFieldCount(); i++) {
      if (matcher.test(inputSchema.getField(i).getName())) {
        result.add(i);
      }
    }
    return result;
  }
}
