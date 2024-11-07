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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.PredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

public class Filters {

  public static PredicateExpression construct(
      Filter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    filter = LogicalFilterUtils.removeNot(filter);
    switch (filter.getType()) {
      case Value:
        return construct((ValueFilter) filter, context, inputSchema);
      case And:
        return construct((AndFilter) filter, context, inputSchema);
      case Or:
        return construct((OrFilter) filter, context, inputSchema);
      case Bool:
        BoolFilter boolFilter = (BoolFilter) filter;
        if (boolFilter.isTrue()) {
          return new TrueNode(context.getConstantPool());
        } else {
          return new FalseNode(context.getConstantPool());
        }
      case Key:
        return construct((KeyFilter) filter, context, inputSchema);
      case Expr:
        return construct((ExprFilter) filter, context, inputSchema);
      case Path:
        return construct((PathFilter) filter, context, inputSchema);
      default:
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
    }
  }

  private static PredicateExpression construct(
      AndFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return and(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static PredicateExpression construct(
      OrFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return or(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static PredicateExpression construct(
      KeyFilter keyFilter, ExecutorContext context, BatchSchema inputSchema) {
    if (!inputSchema.hasKey()) {
      return new FalseNode(context.getConstantPool());
    }
    return new CompareNode(
        getPredicate(keyFilter.getOp()),
        new FieldNode(inputSchema.getKeyIndex()),
        new LiteralNode<>(keyFilter.getValue(), context.getConstantPool()));
  }

  private static PredicateExpression construct(
      ExprFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return new CompareNode(
        getPredicate(filter.getOp()),
        Expressions.getPhysicalExpression(context, inputSchema, filter.getExpressionA()),
        Expressions.getPhysicalExpression(context, inputSchema, filter.getExpressionB()));
  }

  private static PredicateExpression construct(
      PathFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    if (inputSchema.indexOf(filter.getPathA()) == null
        || inputSchema.indexOf(filter.getPathB()) == null) {
      throw new ComputeException("Trying to compare non-existing path(s).");
    }
    return new CompareNode(
        getPredicate(filter.getOp()),
        new FieldNode(inputSchema.indexOf(filter.getPathA())),
        new FieldNode(inputSchema.indexOf(filter.getPathB())));
  }

  private static List<PredicateExpression> construct(
      List<Filter> filters, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<PredicateExpression> result = new ArrayList<>();
    for (Filter filter : filters) {
      result.add(construct(filter, context, inputSchema));
    }
    return result;
  }

  private static PredicateExpression and(
      List<PredicateExpression> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(true), context, inputSchema);
    } else if (children.size() == 1) {
      return children.get(0);
    }
    return new AndNode(children);
  }

  private static PredicateExpression or(
      List<PredicateExpression> children, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(false), context, inputSchema);
    } else if (children.size() == 1) {
      return children.get(0);
    }
    return new OrNode(children);
  }

  private static PredicateExpression construct(
      ValueFilter filter, ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<Integer> paths = Schemas.matchPattern(inputSchema.raw(), filter.getPath());
    if (paths.isEmpty()) {
      throw new ComputeException("Path not found: " + filter.getPath() + " in " + inputSchema);
    }
    List<PredicateExpression> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      Optional<PredicateFunction> predicateFunction =
          getPredicate(filter.getOp(), filter.getValue());
      if (predicateFunction.isPresent()) {
        comparisons.add(new CompareNode(predicateFunction.get(), new FieldNode(pathIndex)));
      } else {
        comparisons.add(
            new CompareNode(
                getPredicate(filter.getOp()),
                new FieldNode(pathIndex),
                new LiteralNode<>(filter.getValue().getValue(), context.getConstantPool())));
      }
    }
    if (Op.isOrOp(filter.getOp())) {
      return or(comparisons, context, inputSchema);
    } else if (Op.isAndOp(filter.getOp())) {
      return and(comparisons, context, inputSchema);
    } else {
      throw new UnsupportedOperationException("Unsupported operator: " + filter.getOp());
    }
  }

  private static Optional<PredicateFunction> getPredicate(Op op, Value value)
      throws ComputeException {
    switch (op) {
      case E:
      case E_AND:
        switch (value.getDataType()) {
          case BINARY:
            return Optional.of(new EqualConstBinary(value.getBinaryV()));
        }
        break;
      case LIKE:
      case LIKE_AND:
        if (value.getDataType() != DataType.BINARY) {
          throw new ComputeException("Unsupported data type for LIKE: " + value.getDataType());
        }
        return Optional.of(new LikeConst(value.getBinaryV()));
      case NOT_LIKE:
      case NOT_LIKE_AND:
        if (value.getDataType() != DataType.BINARY) {
          throw new ComputeException("Unsupported data type for NOT LIKE: " + value.getDataType());
        }
        return Optional.of(new NotLikeConst(value.getBinaryV()));
      default:
        break;
    }
    return Optional.empty();
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

  public static void parseJoinFilter(
      Filter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Op> pathPairOps,
      Set<Integer> leftOnFieldIndices,
      Set<Integer> rightOnFieldIndices)
      throws ComputeException {
    switch (filter.getType()) {
      case Value:
        parseJoinFilter(
            (ValueFilter) filter,
            leftSchema,
            rightSchema,
            pathPairOps,
            leftOnFieldIndices,
            rightOnFieldIndices);
        break;
      case Path:
        parseJoinFilter(
            (PathFilter) filter,
            leftSchema,
            rightSchema,
            pathPairOps,
            leftOnFieldIndices,
            rightOnFieldIndices);
        break;
      case Bool:
        break;
      case And:
        parseJoinFilter(
            (AndFilter) filter,
            leftSchema,
            rightSchema,
            pathPairOps,
            leftOnFieldIndices,
            rightOnFieldIndices);
        break;
      case Key:
      case Expr:
      case Or:
      case Not:
      default:
        throw new IllegalStateException("Unexpected value: " + filter.getType());
    }
  }

  private static void parseJoinFilter(
      AndFilter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Op> pathPairOps,
      Set<Integer> leftOnFieldIndices,
      Set<Integer> rightOnFieldIndices)
      throws ComputeException {
    for (Filter subFilter : filter.getChildren()) {
      parseJoinFilter(
          subFilter, leftSchema, rightSchema, pathPairOps, leftOnFieldIndices, rightOnFieldIndices);
    }
  }

  private static void parseJoinFilter(
      ValueFilter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Op> pathPairOps,
      Set<Integer> leftOnFieldIndices,
      Set<Integer> rightOnFieldIndices)
      throws ComputeException {

    List<Integer> leftMatchedIndices = Schemas.matchPattern(leftSchema.raw(), filter.getPath());
    List<Integer> rightMatchedIndices = Schemas.matchPattern(rightSchema.raw(), filter.getPath());

    leftOnFieldIndices.addAll(leftMatchedIndices);
    rightOnFieldIndices.addAll(rightMatchedIndices);
  }

  private static void parseJoinFilter(
      PathFilter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Op> pathPairOps,
      Set<Integer> leftOnFieldIndices,
      Set<Integer> rightOnFieldIndices)
      throws ComputeException {

    List<Integer> leftAMatchedIndices = Schemas.matchPattern(leftSchema.raw(), filter.getPathA());
    List<Integer> leftBMatchedIndices = Schemas.matchPattern(leftSchema.raw(), filter.getPathB());
    List<Integer> rightAMatchedIndices = Schemas.matchPattern(rightSchema.raw(), filter.getPathA());
    List<Integer> rightBMatchedIndices = Schemas.matchPattern(rightSchema.raw(), filter.getPathB());

    if (leftAMatchedIndices.size() == 1 && rightBMatchedIndices.size() == 1) {
      pathPairOps.put(
          Pair.of(leftAMatchedIndices.get(0), rightBMatchedIndices.get(0)), filter.getOp());
      return;
    }

    if (leftBMatchedIndices.size() == 1 && rightAMatchedIndices.size() == 1) {
      pathPairOps.put(
          Pair.of(leftBMatchedIndices.get(0), rightAMatchedIndices.get(0)), filter.getOp());
      return;
    }

    leftOnFieldIndices.addAll(leftAMatchedIndices);
    leftOnFieldIndices.addAll(leftBMatchedIndices);
    rightOnFieldIndices.addAll(rightAMatchedIndices);
    rightOnFieldIndices.addAll(rightBMatchedIndices);
  }
}
