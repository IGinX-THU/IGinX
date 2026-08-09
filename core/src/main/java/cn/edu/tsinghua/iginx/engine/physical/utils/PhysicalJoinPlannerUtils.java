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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinArrayList;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinHashMap;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash.Hash;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.Not;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.CollectionJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;

public class PhysicalJoinPlannerUtils {

  public static Filter constructFilter(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      AbstractJoin join,
      @Nullable Filter filter,
      List<String> joinColumns)
      throws ComputeException {
    List<Filter> subFilters = new ArrayList<>();
    if (filter != null) {
      subFilters.add(filter);
    }
    List<Pair<String, String>> eqColumnPairs = new ArrayList<>();
    for (String extraPrefix : join.getExtraJoinPrefix()) {
      eqColumnPairs.add(Pair.of(extraPrefix, extraPrefix));
    }
    for (String joinColumn : joinColumns) {
      eqColumnPairs.add(
          Pair.of(
              join.getPrefixA() + SQLConstant.DOT + joinColumn,
              join.getPrefixB() + SQLConstant.DOT + joinColumn));
    }
    for (Pair<String, String> eqColumnPair : eqColumnPairs) {
      String columnA = eqColumnPair.getLeft();
      String columnB = eqColumnPair.getRight();
      if (Schemas.matchPatternIgnoreKey(leftSchema.raw(), columnA).isEmpty()) {
        throw new ComputeException("Path " + columnA + " not found in " + leftSchema.raw());
      }
      if (Schemas.matchPatternIgnoreKey(rightSchema.raw(), columnB).isEmpty()) {
        throw new ComputeException("Path " + columnB + " not found in " + rightSchema.raw());
      }
      subFilters.add(new PathFilter(columnA, Op.E, columnB));
    }
    return new AndFilter(subFilters);
  }

  public static Filter parseJoinFilter(
      Filter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps)
      throws ComputeException {
    switch (filter.getType()) {
      case Path:
        return parseJoinFilter((PathFilter) filter, leftSchema, rightSchema, pathPairOps);
      case And:
        return parseJoinFilter((AndFilter) filter, leftSchema, rightSchema, pathPairOps);
      case Bool:
        BoolFilter boolFilter = (BoolFilter) filter;
        if (boolFilter.isTrue()) {
          return new AndFilter(Collections.emptyList());
        } else {
          return new OrFilter(Collections.emptyList());
        }
      default:
        return filter;
    }
  }

  private static Filter parseJoinFilter(
      AndFilter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps)
      throws ComputeException {
    List<Filter> children = new ArrayList<>();
    for (Filter subFilter : filter.getChildren()) {
      Filter parsedFilter = parseJoinFilter(subFilter, leftSchema, rightSchema, pathPairOps);
      children.add(parsedFilter);
    }
    return new AndFilter(children);
  }

  private static Filter parseJoinFilter(
      PathFilter filter,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps) {

    List<Integer> leftAMatchedIndices =
        Schemas.matchPatternIgnoreKey(leftSchema.raw(), filter.getPathA());
    List<Integer> leftBMatchedIndices =
        Schemas.matchPatternIgnoreKey(leftSchema.raw(), filter.getPathB());
    List<Integer> rightAMatchedIndices =
        Schemas.matchPatternIgnoreKey(rightSchema.raw(), filter.getPathA());
    List<Integer> rightBMatchedIndices =
        Schemas.matchPatternIgnoreKey(rightSchema.raw(), filter.getPathB());

    if (leftAMatchedIndices.size() == 1 && rightBMatchedIndices.size() == 1) {
      pathPairOps.put(
          Pair.of(leftAMatchedIndices.get(0), rightBMatchedIndices.get(0)),
          Pair.of(filter.getOp(), false));
      return new AndFilter(Collections.emptyList());
    }

    if (rightAMatchedIndices.size() == 1 && leftBMatchedIndices.size() == 1) {
      pathPairOps.put(
          Pair.of(leftBMatchedIndices.get(0), rightAMatchedIndices.get(0)),
          Pair.of(filter.getOp(), true));
      return new AndFilter(Collections.emptyList());
    }

    return filter;
  }

  public static PredicateExpression constructJoinMatcher(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Filter filterWithoutPathPairOpsInnerAndFilter,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps)
      throws ComputeException {
    // get matcher
    List<PredicateExpression> matchers = new ArrayList<>();
    for (Map.Entry<Pair<Integer, Integer>, Pair<Op, Boolean>> entry : pathPairOps.entrySet()) {
      Pair<Integer, Integer> pathPair = entry.getKey();
      Pair<Op, Boolean> opEntry = entry.getValue();
      Op op = opEntry.getKey();
      boolean operandIsReversed = opEntry.getValue();
      if (operandIsReversed) {
        matchers.add(
            PhysicalFilterPlannerUtils.construct(
                pathPair.getValue() + leftSchema.getFieldCount(), pathPair.getKey(), op));
      } else {
        matchers.add(
            PhysicalFilterPlannerUtils.construct(
                pathPair.getKey(), pathPair.getValue() + leftSchema.getFieldCount(), op));
      }
    }
    PredicateExpression otherMatcher =
        PhysicalFilterPlannerUtils.construct(
            filterWithoutPathPairOpsInnerAndFilter,
            context,
            Schemas.merge(leftSchema.raw(), rightSchema.raw()));
    matchers.add(otherMatcher);
    return PhysicalFilterPlannerUtils.and(matchers, context);
  }

  public static List<ScalarExpression<?>> constructOutputExpressions(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      AbstractJoin join,
      JoinOption option,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps,
      @Nullable String leftPrefix,
      @Nullable String rightPrefix,
      List<String> joinColumns,
      @Nullable String markColumnName,
      boolean antiMark)
      throws ComputeException {

    // get field indices
    Set<String> sameNameEqualPaths =
        pathPairOps.entrySet().stream()
            .filter(entry -> entry.getValue().getKey() == Op.E)
            .map(Map.Entry::getKey)
            .filter(
                pair ->
                    Objects.equals(
                        leftSchema.getName(pair.getLeft()), rightSchema.getName(pair.getRight())))
            .map(pair -> leftSchema.getName(pair.getLeft()))
            .collect(Collectors.toSet());

    List<ScalarExpression<?>> outputExpressions = new ArrayList<>();

    if (!option.isToOutputBuildSideKey()) {
      if (rightSchema.hasKey()) {
        outputExpressions.add(
            new FieldNode(leftSchema.getFieldCount() + rightSchema.getKeyIndex()));
      }
    }

    Set<String> buildSideIgnoreFields = new HashSet<>();
    Set<String> probeSideIgnoreFields = new HashSet<>();
    for (String joinColumn : joinColumns) {
      buildSideIgnoreFields.add(leftPrefix + "." + joinColumn);
      probeSideIgnoreFields.add(rightPrefix + "." + joinColumn);
    }

    boolean cutProbeSide =
        option.isToOutputBuildSideData()
            && option.isToOutputBuildSideUnmatched()
            && !option.isToOutputProbeSideUnmatched();

    if (option.isToOutputBuildSideData()) {
      outputExpressions.addAll(
          getOutputExpressions(
              0,
              leftSchema,
              join.getPrefixA(),
              option.isToOutputBuildSideKey(),
              f -> {
                if (sameNameEqualPaths.contains(f.getName())) {
                  return true;
                }
                if (!cutProbeSide) {
                  return buildSideIgnoreFields.contains(f.getName());
                }
                return false;
              }));
    }

    outputExpressions.addAll(
        getOutputExpressions(
            leftSchema.getFieldCount(),
            rightSchema,
            join.getPrefixB(),
            option.isToOutputBuildSideKey(),
            f -> {
              if (cutProbeSide) {
                return probeSideIgnoreFields.contains(f.getName());
              }
              return false;
            }));

    if (option.isToOutputMark()) {
      Objects.requireNonNull(markColumnName);
      ScalarExpression<?> markFieldNode =
          new FieldNode(leftSchema.getFieldCount() + rightSchema.getFieldCount(), markColumnName);
      if (antiMark) {
        markFieldNode = new CallNode<>(new Not(), markColumnName, markFieldNode);
      }
      outputExpressions.add(markFieldNode);
    }

    return outputExpressions;
  }

  private static List<ScalarExpression<?>> getOutputExpressions(
      int offset,
      BatchSchema schema,
      @Nullable String prefix,
      boolean outputKeyWhenHasPrefix,
      Predicate<Field> skipFieldTester) {
    List<ScalarExpression<?>> outputExpressions = new ArrayList<>();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (schema.hasKey() && i == schema.getKeyIndex()) {
        if (prefix != null && outputKeyWhenHasPrefix) {
          int keyIndex = schema.getKeyIndex();
          outputExpressions.add(
              new FieldNode(offset + keyIndex, prefix + "." + schema.getName(keyIndex)));
        }
        continue;
      }
      if (skipFieldTester.test(schema.getField(i))) {
        continue;
      }
      outputExpressions.add(new FieldNode(offset + i));
    }
    return outputExpressions;
  }

  public static StatefulBinaryExecutor constructJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      AbstractJoin operator,
      JoinOption joinOption,
      @Nullable Filter filter,
      List<String> joinColumns,
      boolean joinByKey,
      boolean naturalJoin,
      @Nullable String markColumnName,
      boolean antiMark)
      throws ComputeException {
    Preconditions.checkArgument(!joinByKey);
    Preconditions.checkArgument(!naturalJoin);

    Filter unionFilter =
        PhysicalJoinPlannerUtils.constructFilter(
            context, leftSchema, rightSchema, operator, filter, joinColumns);

    Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps = new HashMap<>();
    Filter filterWithoutPathPairOpsInnerAndFilter =
        PhysicalJoinPlannerUtils.parseJoinFilter(unionFilter, leftSchema, rightSchema, pathPairOps);

    PredicateExpression matcher =
        PhysicalJoinPlannerUtils.constructJoinMatcher(
            context, leftSchema, rightSchema, filterWithoutPathPairOpsInnerAndFilter, pathPairOps);

    List<ScalarExpression<?>> outputExpressions =
        PhysicalJoinPlannerUtils.constructOutputExpressions(
            context,
            leftSchema,
            rightSchema,
            operator,
            joinOption,
            pathPairOps,
            operator.getPrefixA(),
            operator.getPrefixB(),
            joinColumns,
            markColumnName,
            antiMark);

    switch (operator.getJoinAlgType()) {
      case NestedLoopJoin:
        return PhysicalJoinPlannerUtils.constructNestedLoopJoin(
            context, leftSchema, rightSchema, joinOption, matcher, outputExpressions);
      case HashJoin:
        return PhysicalJoinPlannerUtils.constructHashJoin(
            context, leftSchema, rightSchema, joinOption, matcher, outputExpressions, pathPairOps);
      default:
        throw new IllegalStateException(
            "JoinAlgType is not supported: " + operator.getJoinAlgType());
    }
  }

  public static StatefulBinaryExecutor constructNestedLoopJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      JoinOption joinOption,
      PredicateExpression matcher,
      List<ScalarExpression<?>> outputExpressions)
      throws ComputeException {

    String info =
        "NestedLoopJoin(" + joinOption + ") on " + matcher + " output " + outputExpressions;

    return new CollectionJoinExecutor(
        context,
        leftSchema,
        rightSchema,
        new JoinArrayList.Builder(
            context.getAllocator(),
            leftSchema.raw(),
            rightSchema.raw(),
            outputExpressions,
            joinOption,
            matcher),
        info);
  }

  public static StatefulBinaryExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      JoinOption joinOption,
      PredicateExpression matcher,
      List<ScalarExpression<?>> outputExpressions,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps)
      throws ComputeException {

    // get hasher
    List<Pair<Integer, Integer>> pathPairEqualOps =
        pathPairOps.entrySet().stream()
            .filter(entry -> entry.getValue().getKey() == Op.E)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    ScalarExpression<IntVector> leftHasher =
        new CallNode<>(
            new Hash(),
            pathPairEqualOps.stream()
                .map(Pair::getLeft)
                .map(FieldNode::new)
                .collect(Collectors.toList()));
    ScalarExpression<IntVector> rightHasher =
        new CallNode<>(
            new Hash(),
            pathPairEqualOps.stream()
                .map(Pair::getRight)
                .map(FieldNode::new)
                .collect(Collectors.toList()));

    String info = "HashJoin(" + joinOption + ") on " + matcher + " output " + outputExpressions;

    return new CollectionJoinExecutor(
        context,
        leftSchema,
        rightSchema,
        new JoinHashMap.Builder(
            context.getAllocator(),
            leftSchema.raw(),
            rightSchema.raw(),
            outputExpressions,
            joinOption,
            matcher,
            leftHasher,
            rightHasher),
        info);
  }
}
