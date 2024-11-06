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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.Equal;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

public class HashJoins {

  public static HashJoinExecutor constructHashJoin(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      Filter filter,
      JoinOption joinOption)
      throws ComputeException {

    Map<Pair<Integer, Integer>, Op> pathPairOps = new HashMap<>();
    Set<Integer> leftOnFieldIndices = new HashSet<>();
    Set<Integer> rightOnFieldIndices = new HashSet<>();

    if (!filter.equals(new BoolFilter(true))) {
      Filters.parseJoinFilter(
          filter, leftSchema, rightSchema, pathPairOps, leftOnFieldIndices, rightOnFieldIndices);
    }

    List<Pair<Integer, Integer>> sameNameIndicesPairs = new ArrayList<>();
    Set<String> sameNameEqualPaths = new HashSet<>();

    pathPairOps
        .keySet()
        .forEach(
            pathPair -> {
              if (Objects.equals(
                  leftSchema.getName(pathPair.getLeft()),
                  rightSchema.getName(pathPair.getRight()))) {
                sameNameEqualPaths.add(leftSchema.getName(pathPair.getLeft()));
                sameNameIndicesPairs.add(pathPair);
              }
              leftOnFieldIndices.add(pathPair.getLeft());
              rightOnFieldIndices.add(pathPair.getRight());
            });

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

    // get on field indices
    List<Integer> leftOnFieldIndicesList =
        leftOnFieldIndices.stream().sorted().collect(Collectors.toList());
    List<Integer> rightOnFieldIndicesListWithoutSameNameField =
        rightOnFieldIndices.stream()
            .sorted()
            .filter(i -> !sameNameEqualPaths.contains(rightSchema.getName(i)))
            .collect(Collectors.toList());
    List<Integer> rightOnFieldIndicesList =
        new ArrayList<>(rightOnFieldIndicesListWithoutSameNameField);
    sameNameIndicesPairs.forEach(pair -> rightOnFieldIndicesList.add(pair.getRight()));

    // get on fields pair tester without same name fields
    Schema leftOnFieldsSchema =
        new Schema(
            leftOnFieldIndicesList.stream()
                .map(leftSchema.raw().getFields()::get)
                .collect(Collectors.toList()));
    Schema rightOnFieldsSchemaWithoutSameNameField =
        new Schema(
            rightOnFieldIndicesListWithoutSameNameField.stream()
                .map(rightSchema.raw().getFields()::get)
                .collect(Collectors.toList()));
    Schema rightOnFieldsSchema =
        new Schema(
            rightOnFieldIndicesList.stream()
                .map(rightSchema.raw().getFields()::get)
                .collect(Collectors.toList()));

    // get on fields pair tester
    List<Pair<Integer, Integer>> OnFieldsSameNameIndicesPairs =
        getSameNameIndicesPairs(sameNameEqualPaths, leftOnFieldsSchema, rightOnFieldsSchema);
    List<ScalarExpression<BitVector>> onFieldsPairTesters =
        OnFieldsSameNameIndicesPairs.stream()
            .map(
                pair ->
                    new CallNode<>(
                        new Equal(),
                        new FieldNode(pair.getLeft()),
                        new FieldNode(pair.getRight() + leftOnFieldsSchema.getFields().size())))
            .collect(Collectors.toList());

    if (!filter.equals(new BoolFilter(true))) {
      ScalarExpression<BitVector> onFieldsPairTesterWithoutSameNameFields =
          Filters.construct(
              filter,
              context,
              BatchSchema.of(
                  Schemas.merge(leftOnFieldsSchema, rightOnFieldsSchemaWithoutSameNameField)));
      onFieldsPairTesters.add(onFieldsPairTesterWithoutSameNameFields);
    }

    ScalarExpression<BitVector> onFieldsPairTester =
        onFieldsPairTesters.stream()
            .reduce((left, right) -> new CallNode<>(new And(), left, right))
            .orElseThrow(() -> new IllegalStateException("onFieldsPairTesters is empty"));

    // get left and right of on fields pair expressions
    List<FieldNode> leftOnFieldExpressions =
        leftOnFieldIndicesList.stream().map(FieldNode::new).collect(Collectors.toList());
    List<FieldNode> rightOnFieldExpressions =
        rightOnFieldIndicesList.stream().map(FieldNode::new).collect(Collectors.toList());

    // get left and right output expressions
    int leftFieldStartIndex = leftSchema.hasKey() ? 1 : 0;
    int rightFieldStartIndex = rightSchema.hasKey() ? 1 : 0;
    List<ScalarExpression<?>> leftOutputExpressions =
        IntStream.range(leftFieldStartIndex, leftSchema.raw().getFields().size())
            .filter(i -> !sameNameEqualPaths.contains(leftSchema.getName(i)))
            .mapToObj(FieldNode::new)
            .collect(Collectors.toList());
    List<ScalarExpression<?>> rightOutputExpressions =
        IntStream.range(rightFieldStartIndex, rightSchema.raw().getFields().size())
            .mapToObj(FieldNode::new)
            .collect(Collectors.toList());

    return new HashJoinExecutor(
        context,
        leftSchema,
        rightSchema,
        joinOption,
        joinOption.needMark() ? Collections.emptyList() : leftOutputExpressions,
        rightOutputExpressions,
        leftOnFieldExpressions,
        rightOnFieldExpressions,
        onFieldsPairTester,
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
