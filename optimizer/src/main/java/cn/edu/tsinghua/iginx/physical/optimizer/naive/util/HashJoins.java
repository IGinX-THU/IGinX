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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash.Hash;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashJoins {

  public static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Filter filter,
      JoinOption joinOption)
      throws ComputeException {

    Map<Pair<Integer, Integer>, Op> pathPairOps = new HashMap<>();
    Filter filterWithoutPathPairOpsInnerAndFilter = Filters.parseJoinFilter(filter, leftSchema, rightSchema, pathPairOps);

    return constructHashJoin(
        context,
        leftSchema,
        rightSchema,
        filterWithoutPathPairOpsInnerAndFilter,
        pathPairOps,
        joinOption);
  }

  private static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Filter filterWithoutPathPairOpsInnerAndFilter,
      Map<Pair<Integer, Integer>, Op> pathPairOps,
      JoinOption joinOption)
      throws ComputeException {

    // get matcher
    List<PredicateExpression> matchers = new ArrayList<>();
    for (Map.Entry<Pair<Integer, Integer>, Op> entry : pathPairOps.entrySet()) {
      Pair<Integer, Integer> pathPair = entry.getKey();
      Op op = entry.getValue();
      matchers.add(Filters.construct(pathPair.getLeft(), pathPair.getRight() + leftSchema.getFieldCount(), op));
    }
    PredicateExpression otherMatcher = Filters.construct(
        filterWithoutPathPairOpsInnerAndFilter,
        context,
        BatchSchema.of(Schemas.merge(leftSchema.raw(), rightSchema.raw()))
    );
    matchers.add(otherMatcher);
    PredicateExpression matcher = Filters.and(matchers);

    // get hasher
    Map<Pair<Integer, Integer>, Op> pathPairEqualOps =
        pathPairOps.entrySet().stream()
            .filter(entry -> entry.getValue() == Op.E)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ScalarExpression<IntVector> leftHasher =
        new CallNode<>(
            new Hash(),
            pathPairEqualOps.keySet().stream()
                .map(Pair::getLeft)
                .map(FieldNode::new)
                .collect(Collectors.toList()));
    ScalarExpression<IntVector> rightHasher =
        new CallNode<>(
            new Hash(),
            pathPairEqualOps.keySet().stream()
                .map(Pair::getRight)
                .map(FieldNode::new)
                .collect(Collectors.toList()));

    // get field indices
    Set<String> sameNameEqualPaths = pathPairEqualOps.keySet().stream()
        .filter(pair -> Objects.equals(leftSchema.getName(pair.getLeft()), rightSchema.getName(pair.getRight())))
        .map(pair -> leftSchema.getName(pair.getLeft()))
        .collect(Collectors.toSet());
    List<ScalarExpression<?>> outputExpressions = new ArrayList<>();
    if (!joinOption.needMark()) {
      outputExpressions.addAll(
          IntStream.range(0, leftSchema.getFieldCount())
              .filter(i -> !sameNameEqualPaths.contains(leftSchema.getName(i)))
              .mapToObj(FieldNode::new)
              .collect(Collectors.toList())
      );
    }
    outputExpressions.addAll(
        IntStream.range(0, rightSchema.getFieldCount())
            .mapToObj(i -> new FieldNode(i + leftSchema.getFieldCount()))
            .collect(Collectors.toList())
    );
    if (joinOption.needMark()) {
      outputExpressions.add(new FieldNode(leftSchema.getFieldCount() + rightSchema.getFieldCount(), joinOption.markColumnName()));
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

  private static List<Pair<Integer, Integer>> getSameNameIndicesPairs(
      Collection<String> sameNameEqualPaths, Schema leftSchema, Schema rightSchema)
      throws ComputeException {
    List<Pair<Integer, Integer>> sameNameIndicesPairs = new ArrayList<>();
    for (String sameNameEqualPath : sameNameEqualPaths) {
      List<Integer> leftPath = Schemas.matchPattern(leftSchema, sameNameEqualPath);
      List<Integer> rightPath = Schemas.matchPattern(rightSchema, sameNameEqualPath);
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
