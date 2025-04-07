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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.PredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.constant.EqualConst;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.constant.LikeConst;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.constant.NotEqualConst;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.constant.NotLikeConst;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class PhysicalFilterPlannerUtils {

  public static PredicateExpression construct(
      Filter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    filter = LogicalFilterUtils.removeNot(filter);
    filter = reorderFilter(filter);
    switch (filter.getType()) {
      case Value:
        return construct((ValueFilter) filter, context, inputSchema);
      case And:
        return construct((AndFilter) filter, context, inputSchema);
      case Or:
        return construct((OrFilter) filter, context, inputSchema);
      case Bool:
        return construct((BoolFilter) filter, context, inputSchema);
      case Key:
        return construct((KeyFilter) filter, context, inputSchema);
      case Expr:
        return construct((ExprFilter) filter, context, inputSchema);
      case Path:
        return construct((PathFilter) filter, context, inputSchema);
      case In:
        return construct((InFilter) filter, context, inputSchema);
      case Not:
      default:
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
    }
  }

  private static PredicateExpression construct(
      BoolFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    if (filter.isTrue()) {
      return new TrueNode(context.getConstantPool());
    } else {
      return new FalseNode(context.getConstantPool());
    }
  }

  private static PredicateExpression construct(
      AndFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    return and(construct(filter.getChildren(), context, inputSchema), context);
  }

  private static PredicateExpression construct(
      OrFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    return or(construct(filter.getChildren(), context, inputSchema), context);
  }

  private static PredicateExpression construct(
      KeyFilter keyFilter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    List<Integer> paths = Schemas.matchPattern(inputSchema, BatchSchema.KEY.getName());
    if (paths.isEmpty()) {
      throw new ComputeException(
          "Key not found: " + BatchSchema.KEY.getName() + " in " + inputSchema);
    }
    if (paths.size() > 1) {
      throw new ComputeException("Multiple keys found in " + inputSchema);
    }
    Value value = new Value(DataType.LONG, keyFilter.getValue());
    return construct(paths, keyFilter.getOp(), value, context, inputSchema);
  }

  private static PredicateExpression construct(
      ExprFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    return new CompareNode(
        getPredicate(filter.getOp()),
        PhysicalExpressionPlannerUtils.getPhysicalExpression(
            context, inputSchema, filter.getExpressionA(), false),
        PhysicalExpressionPlannerUtils.getPhysicalExpression(
            context, inputSchema, filter.getExpressionB(), false));
  }

  private static PredicateExpression construct(
      PathFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    List<Integer> pathAs = Schemas.matchPatternIgnoreKey(inputSchema, filter.getPathA());
    List<Integer> pathBs = Schemas.matchPatternIgnoreKey(inputSchema, filter.getPathB());
    if (pathAs.isEmpty() || pathBs.isEmpty()) {
      throw new ComputeException("Trying to compare non-existing path(s).");
    }
    if (pathAs.size() > 1 || pathBs.size() > 1) {
      throw new ComputeException("Trying to compare multiple paths.");
    }
    return construct(pathAs.get(0), pathBs.get(0), filter.getOp());
  }

  public static PredicateExpression construct(int left, int right, Op op) {
    return new CompareNode(getPredicate(op), new FieldNode(left), new FieldNode(right));
  }

  private static List<PredicateExpression> construct(
      List<Filter> filters, ExecutorContext context, Schema inputSchema) throws ComputeException {
    List<PredicateExpression> result = new ArrayList<>();
    for (Filter filter : filters) {
      result.add(construct(filter, context, inputSchema));
    }
    return result;
  }

  public static PredicateExpression and(
      List<PredicateExpression> children, ExecutorContext context) {
    List<PredicateExpression> subPredicates =
        children.stream()
            .map(PhysicalFilterPlannerUtils::getAndNodeSubPredicates)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    if (subPredicates.isEmpty()) {
      return new TrueNode(context.getConstantPool());
    }
    if (subPredicates.size() == 1) {
      return subPredicates.get(0);
    }
    if (subPredicates.stream().anyMatch(e -> e instanceof FalseNode)) {
      return new FalseNode(context.getConstantPool());
    }
    return new AndNode(subPredicates);
  }

  public static List<PredicateExpression> getAndNodeSubPredicates(PredicateExpression filter) {
    if (filter instanceof AndNode) {
      List<PredicateExpression> result = new ArrayList<>();
      for (PredicateExpression subFilter : ((AndNode) filter).getSubPredicates()) {
        result.addAll(getAndNodeSubPredicates(subFilter));
      }
      return result;
    }
    if (filter instanceof TrueNode) {
      return Collections.emptyList();
    }
    return Collections.singletonList(filter);
  }

  public static PredicateExpression or(
      List<PredicateExpression> children, ExecutorContext context) {
    List<PredicateExpression> subPredicates =
        children.stream()
            .map(PhysicalFilterPlannerUtils::getOrNodeSubPredicates)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    if (subPredicates.isEmpty()) {
      return new FalseNode(context.getConstantPool());
    }
    if (subPredicates.size() == 1) {
      return children.get(0);
    }
    if (subPredicates.stream().anyMatch(e -> e instanceof TrueNode)) {
      return new TrueNode(context.getConstantPool());
    }
    return new OrNode(children);
  }

  public static List<PredicateExpression> getOrNodeSubPredicates(PredicateExpression filter) {
    if (filter instanceof OrNode) {
      List<PredicateExpression> result = new ArrayList<>();
      for (PredicateExpression subFilter : ((OrNode) filter).getSubPredicates()) {
        result.addAll(getAndNodeSubPredicates(subFilter));
      }
      return result;
    }
    if (filter instanceof FalseNode) {
      return Collections.emptyList();
    }
    return Collections.singletonList(filter);
  }

  private static PredicateExpression construct(
      ValueFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    List<Integer> paths = Schemas.matchPatternIgnoreKey(inputSchema, filter.getPath());
    if (paths.isEmpty()) {
      throw new ComputeException("Path not found: " + filter.getPath() + " in " + inputSchema);
    }
    return construct(paths, filter.getOp(), filter.getValue(), context, inputSchema);
  }

  private static PredicateExpression construct(
      List<Integer> paths, Op op, Value value, ExecutorContext context, Schema inputSchema)
      throws ComputeException {
    List<PredicateExpression> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      PredicateFunction predicateFunction = getPredicate(op, value);
      if (predicateFunction != null) {
        comparisons.add(new CompareNode(predicateFunction, new FieldNode(pathIndex)));
      } else {
        comparisons.add(
            new CompareNode(
                getPredicate(op),
                new FieldNode(pathIndex),
                new LiteralNode<>(value.getValue(), context.getConstantPool())));
      }
    }
    if (Op.isOrOp(op)) {
      return or(comparisons, context);
    } else if (Op.isAndOp(op)) {
      return and(comparisons, context);
    } else {
      throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }

  @Nullable
  private static PredicateFunction getPredicate(Op op, Value value) throws ComputeException {
    switch (op) {
      case E:
      case E_AND:
        return new EqualConst(value.getValue());
      case NE:
      case NE_AND:
        return new NotEqualConst(value.getValue());
      case LIKE:
      case LIKE_AND:
        if (value.getDataType() != DataType.BINARY) {
          throw new ComputeException("Unsupported data type for LIKE: " + value.getDataType());
        }
        return new LikeConst(value.getBinaryV());
      case NOT_LIKE:
      case NOT_LIKE_AND:
        if (value.getDataType() != DataType.BINARY) {
          throw new ComputeException("Unsupported data type for NOT LIKE: " + value.getDataType());
        }
        return new NotLikeConst(value.getBinaryV());
      default:
        break;
    }
    return null;
  }

  private static PredicateFunction getPredicate(Op op) {
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
        return new Like();
      case NOT_LIKE:
      case NOT_LIKE_AND:
        return new NotLike();
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }

  private static PredicateExpression construct(
      InFilter filter, ExecutorContext context, Schema inputSchema) throws ComputeException {
    List<Integer> paths = Schemas.matchPatternIgnoreKey(inputSchema, filter.getPath());
    if (paths.isEmpty()) {
      throw new ComputeException("Path not found: " + filter.getPath() + " in " + inputSchema);
    }
    List<PredicateExpression> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      Field field = inputSchema.getFields().get(pathIndex);
      DataType type = Schemas.toDataType(field.getType());
      PredicateFunction predicateFunction =
          getPredicate(type, filter.getInOp().isNotOp(), filter.getValues());
      comparisons.add(new CompareNode(predicateFunction, new FieldNode(pathIndex)));
    }
    if (filter.getInOp().isOrOp()) {
      return or(comparisons, context);
    } else {
      return and(comparisons, context);
    }
  }

  private static PredicateFunction getPredicate(DataType type, boolean isNotIn, Set<Value> values)
      throws ComputeException {
    HashSet<Object> filteredValues = new HashSet<>();
    for (Value value : values) {
      if (value.getDataType() == type) {
        filteredValues.add(value.getValue());
      }
    }
    if (isNotIn) {
      return new NotInSet(filteredValues);
    } else {
      return new InSet(filteredValues);
    }
  }

  private static Filter reorderFilter(Filter filter) {
    switch (filter.getType()) {
      case And:
        AndFilter andFilter = (AndFilter) filter;
        List<Filter> reorderedAndChildren =
            andFilter.getChildren().stream()
                .map(PhysicalFilterPlannerUtils::reorderFilter)
                .sorted(Comparator.comparingInt(PhysicalFilterPlannerUtils::countComplexFilter))
                .collect(Collectors.toList());
        return new AndFilter(reorderedAndChildren);
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        List<Filter> reorderedOrChildren =
            orFilter.getChildren().stream()
                .map(PhysicalFilterPlannerUtils::reorderFilter)
                .sorted(Comparator.comparingInt(PhysicalFilterPlannerUtils::countComplexFilter))
                .collect(Collectors.toList());
        return new OrFilter(reorderedOrChildren);
      default:
        return filter;
    }
  }

  private static int countComplexFilter(Filter filter) {
    int[] count = {0};
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            if (filter.getValue().getDataType() == DataType.BINARY) {
              count[0]++;
            }
          }

          @Override
          public void visit(PathFilter filter) {}

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            count[0]++;
          }

          @Override
          public void visit(InFilter filter) {
            if (filter.getValues().stream().anyMatch(v -> v.getDataType() == DataType.BINARY)) {
              count[0]++;
            }
          }
        });
    return count[0];
  }
}
