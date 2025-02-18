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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

public class HashJoinUtils {

  public static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      @Nullable String leftPrefix,
      @Nullable String rightPrefix,
      Filter filter,
      Set<String> ignoreFields,
      JoinOption joinOption)
      throws ComputeException {
    return constructHashJoin(
        context,
        leftSchema,
        rightSchema,
        leftPrefix,
        rightPrefix,
        filter,
        ignoreFields,
        joinOption,
        "&mark",
        false,
        true,
        true);
  }

  public static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      @Nullable String leftPrefix,
      @Nullable String rightPrefix,
      Filter filter,
      Set<String> ignoreFields,
      JoinOption joinOption,
      String markColumnName,
      boolean antiMark,
      boolean outputBuildSideData,
      boolean outputBuildSideKey)
      throws ComputeException {

    Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps = new HashMap<>();
    Filter filterWithoutPathPairOpsInnerAndFilter =
        PhysicalFilterUtils.parseJoinFilter(filter, leftSchema, rightSchema, pathPairOps);

    return constructHashJoin(
        context,
        leftSchema,
        rightSchema,
        leftPrefix,
        rightPrefix,
        LogicalFilterUtils.foldConst(filterWithoutPathPairOpsInnerAndFilter),
        ignoreFields,
        pathPairOps,
        joinOption,
        markColumnName,
        antiMark,
        outputBuildSideData,
        outputBuildSideKey);
  }

  private static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      @Nullable String leftPrefix,
      @Nullable String rightPrefix,
      Filter filterWithoutPathPairOpsInnerAndFilter,
      Set<String> ignoreFields,
      Map<Pair<Integer, Integer>, Pair<Op, Boolean>> pathPairOps,
      JoinOption joinOption,
      String markColumnName,
      boolean antiMark,
      boolean outputBuildSideData,
      boolean outputBuildSideKey)
      throws ComputeException {

    // get matcher
    List<PredicateExpression> matchers = new ArrayList<>();
    for (Map.Entry<Pair<Integer, Integer>, Pair<Op, Boolean>> entry : pathPairOps.entrySet()) {
      Pair<Integer, Integer> pathPair = entry.getKey();
      Pair<Op, Boolean> opEntry = entry.getValue();
      Op op = opEntry.getKey();
      boolean keyIsLeft = opEntry.getValue();
      if (keyIsLeft) {
        matchers.add(
            PhysicalFilterUtils.construct(
                pathPair.getKey(), pathPair.getValue() + leftSchema.getFieldCount(), op));
      } else {
        matchers.add(
            PhysicalFilterUtils.construct(
                pathPair.getKey() + leftSchema.getFieldCount(), pathPair.getValue(), op));
      }
    }
    PredicateExpression otherMatcher =
        PhysicalFilterUtils.construct(
            filterWithoutPathPairOpsInnerAndFilter,
            context,
            Schemas.merge(leftSchema.raw(), rightSchema.raw()));
    matchers.add(otherMatcher);
    PredicateExpression matcher = PhysicalFilterUtils.and(matchers, context);

    // get hasher
    List<Pair<Integer, Integer>> pathPairEqualOps =
        pathPairOps.entrySet().stream()
            .filter(entry -> entry.getValue().getKey() == Op.E)
            .map(
                e ->
                    e.getValue().getValue()
                        ? e.getKey()
                        : Pair.of(e.getKey().getRight(), e.getKey().getLeft()))
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

    // get field indices
    Set<String> sameNameEqualPaths =
        pathPairEqualOps.stream()
            .filter(
                pair ->
                    Objects.equals(
                        leftSchema.getName(pair.getLeft()), rightSchema.getName(pair.getRight())))
            .map(pair -> leftSchema.getName(pair.getLeft()))
            .collect(Collectors.toSet());
    List<ScalarExpression<?>> outputExpressions = new ArrayList<>();

    if (!outputBuildSideKey) {
      if (rightSchema.hasKey()) {
        outputExpressions.add(
            new FieldNode(leftSchema.getFieldCount() + rightSchema.getKeyIndex()));
      }
    }

    if (outputBuildSideData) {
      outputExpressions.addAll(
          getOutputExpressions(
              0,
              leftSchema,
              leftPrefix,
              outputBuildSideKey,
              f -> sameNameEqualPaths.contains(f.getName()) || ignoreFields.contains(f.getName())));
    }

    outputExpressions.addAll(
        getOutputExpressions(
            leftSchema.getFieldCount(),
            rightSchema,
            rightPrefix,
            outputBuildSideKey,
            f -> ignoreFields.contains(f.getName())));

    if (joinOption.isToOutputMark()) {
      ScalarExpression<?> markFieldNode =
          new FieldNode(leftSchema.getFieldCount() + rightSchema.getFieldCount(), markColumnName);
      if (antiMark) {
        markFieldNode = new CallNode<>(new Not(), markColumnName, markFieldNode);
      }
      outputExpressions.add(markFieldNode);
    }

    return new HashJoinExecutor(
        context,
        leftSchema,
        rightSchema,
        joinOption,
        matcher,
        outputExpressions,
        leftHasher,
        rightHasher);
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

  private static List<Pair<Integer, Integer>> getSameNameIndicesPairs(
      Collection<String> sameNameEqualPaths, Schema leftSchema, Schema rightSchema)
      throws ComputeException {
    List<Pair<Integer, Integer>> sameNameIndicesPairs = new ArrayList<>();
    for (String sameNameEqualPath : sameNameEqualPaths) {
      List<Integer> leftPath = Schemas.matchPatternIgnoreKey(leftSchema, sameNameEqualPath);
      List<Integer> rightPath = Schemas.matchPatternIgnoreKey(rightSchema, sameNameEqualPath);
      if (leftPath.isEmpty() || rightPath.isEmpty()) {
        throw new ComputeException("Path not found: " + sameNameEqualPath);
      }
      if (leftPath.size() > 1 || rightPath.size() > 1) {
        throw new ComputeException("Ambiguous path: " + sameNameEqualPath);
      }
      sameNameIndicesPairs.add(Pair.of(leftPath.get(0), rightPath.get(0)));
    }
    return sameNameIndicesPairs;
  }
}
