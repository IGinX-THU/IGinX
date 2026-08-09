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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import io.netty.util.collection.IntObjectHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinHashMap extends JoinArrayList {
  private final ScalarExpression<IntVector> probeSideHasher;
  private final IntObjectHashMap<List<Integer>> buildSideMap;

  JoinHashMap(
      JoinArrayList parent,
      ScalarExpression<IntVector> probeSideHasher,
      IntObjectHashMap<List<Integer>> buildSideMap) {
    super(parent);
    this.probeSideHasher = probeSideHasher;
    this.buildSideMap = buildSideMap;
  }

  @Override
  protected int outputMatchedBeforeBuildCandidates(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      boolean[] probeSideMatched)
      throws ComputeException {

    try (SelectionBuilder buildSideCandidateIndicesBuilder =
            new SelectionBuilder(allocator, "tempBuildSideIndices", probeSideMatched.length);
        SelectionBuilder probeSideCandidateIndicesBuilder =
            new SelectionBuilder(allocator, "tempProbeSideIndices", probeSideMatched.length)) {

      try (IntVector probeSideHashCodes =
          probeSideHasher.invoke(allocator, probeSideDictionaryProvider, null, probeSideBatch)) {
        for (int probeSideCandidate = 0;
            probeSideCandidate < probeSideMatched.length;
            probeSideCandidate++) {
          int hashCode = probeSideHashCodes.get(probeSideCandidate);
          for (int buildSideCandidate :
              buildSideMap.getOrDefault(hashCode, Collections.emptyList())) {
            buildSideCandidateIndicesBuilder.append(buildSideCandidate);
            probeSideCandidateIndicesBuilder.append(probeSideCandidate);
          }
        }
      }

      try (IntVector buildSideCandidateIndices = buildSideCandidateIndicesBuilder.build();
          IntVector probeSideCandidateIndices = probeSideCandidateIndicesBuilder.build()) {
        return outputMatchedBeforeFilter(
            probeSideDictionaryProvider,
            probeSideBatch,
            buildSideIndicesBuilder,
            probeSideIndicesBuilder,
            buildSideCandidateIndices,
            probeSideCandidateIndices,
            probeSideMatched);
      }
    }
  }

  public static class Builder extends JoinArrayList.Builder {
    private final ScalarExpression<IntVector> buildSideHasher;
    private final ScalarExpression<IntVector> probeSideHasher;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        Schema probeSideSchema,
        List<ScalarExpression<?>> outputExpressions,
        JoinOption joinOption,
        PredicateExpression matcher,
        ScalarExpression<IntVector> buildSideHasher,
        ScalarExpression<IntVector> probeSideHasher) {
      super(allocator, buildSideSchema, probeSideSchema, outputExpressions, joinOption, matcher);
      this.buildSideHasher = Objects.requireNonNull(buildSideHasher);
      this.probeSideHasher = Objects.requireNonNull(probeSideHasher);
    }

    @Override
    public JoinHashMap build(ResultConsumer resultConsumer) throws ComputeException {
      Objects.requireNonNull(resultConsumer);

      IntObjectHashMap<List<Integer>> buildSideMap = new IntObjectHashMap<>();
      try (JoinArrayList joinArrayList = super.build(resultConsumer)) {
        try (IntVector buildSideHashCodes =
            buildSideHasher.invoke(
                joinArrayList.allocator,
                joinArrayList.buildSideDictionaryProvider,
                null,
                joinArrayList.buildSideSingleBatch)) {
          for (int i = 0; i < buildSideHashCodes.getValueCount(); i++) {
            int hashCode = buildSideHashCodes.get(i);
            buildSideMap.computeIfAbsent(hashCode, k -> new ArrayList<>()).add(i);
          }
        }
        return new JoinHashMap(joinArrayList, probeSideHasher, buildSideMap);
      }
    }
  }
}
