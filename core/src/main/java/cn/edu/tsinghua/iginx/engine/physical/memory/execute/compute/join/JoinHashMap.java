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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import io.netty.util.collection.IntObjectHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.StableVectorComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinHashMap implements AutoCloseable {

  private final BufferAllocator allocator;
  private final JoinOption joinOption;
  private final PredicateExpression matcher;
  private final List<ScalarExpression<?>> outputExpressions;
  private final ScalarExpression<IntVector> probeSideHasher;
  private final Schema probeSideSchema;
  private final IntObjectHashMap<List<Integer>> buildSideMap;
  private final VectorSchemaRoot buildSideSingleBatch;
  private final boolean[] buildSideMatched;
  private final ResultConsumer resultConsumer;

  JoinHashMap(
      BufferAllocator allocator,
      JoinOption joinOption,
      PredicateExpression matcher,
      List<ScalarExpression<?>> outputExpressions,
      ScalarExpression<IntVector> probeSideHasher,
      Schema probeSideSchema,
      IntObjectHashMap<List<Integer>> buildSideMap,
      @WillCloseWhenClosed VectorSchemaRoot buildSideSingleBatch,
      ResultConsumer resultConsumer) {
    this.allocator = allocator;
    this.joinOption = joinOption;
    this.matcher = matcher;
    this.outputExpressions = outputExpressions;
    this.probeSideHasher = probeSideHasher;
    this.probeSideSchema = probeSideSchema;
    this.buildSideMap = buildSideMap;
    this.buildSideSingleBatch = buildSideSingleBatch;
    this.buildSideMatched = new boolean[buildSideSingleBatch.getRowCount()];
    this.resultConsumer = resultConsumer;
  }

  @Override
  public void close() {
    buildSideSingleBatch.close();
  }

  public void probe(VectorSchemaRoot probeSideBatch) throws ComputeException {
    Preconditions.checkArgument(probeSideBatch.getSchema().equals(probeSideSchema));

    try (SelectionBuilder buildSideCandidateIndicesBuilder =
            new SelectionBuilder(allocator, "tempBuildSideIndices", probeSideBatch.getRowCount());
        SelectionBuilder probeSideCandidateIndicesBuilder =
            new SelectionBuilder(allocator, "tempProbeSideIndices", probeSideBatch.getRowCount())) {

      try (IntVector probeSideHashCodes = probeSideHasher.invoke(allocator, probeSideBatch)) {
        for (int probeSideCandidate = 0;
            probeSideCandidate < probeSideHashCodes.getValueCount();
            probeSideCandidate++) {
          int hashCode = probeSideHashCodes.get(probeSideCandidate);
          for (int buildSideCandidate :
              buildSideMap.getOrDefault(hashCode, Collections.emptyList())) {
            buildSideCandidateIndicesBuilder.append(buildSideCandidate);
            probeSideCandidateIndicesBuilder.append(probeSideCandidate);
          }
        }
      }

      try (ArrayDictionaryProvider outputDictionaryProvider =
              ArrayDictionaryProvider.of(allocator, buildSideSingleBatch, probeSideBatch);
          IntVector buildSideCandidateIndices = buildSideCandidateIndicesBuilder.build();
          IntVector proSideCandidateIndices = probeSideCandidateIndicesBuilder.build()) {
        boolean[] probeSideMatched = new boolean[probeSideBatch.getRowCount()];
        output(
            outputDictionaryProvider,
            buildSideCandidateIndices,
            proSideCandidateIndices,
            probeSideMatched);
      }
    }
  }

  public void flush() throws ComputeException {
    if (!joinOption.isToOutputBuildSideUnmatched()) {
      return;
    }

    // TODO: 使用 NullVector 优化空值的处理，从而减少访存

    try (SelectionBuilder buildSideIndicesBuilder =
            new SelectionBuilder(
                allocator, "buildSideIndices", buildSideSingleBatch.getRowCount());
        SelectionBuilder probeSideIndicesBuilder =
            new SelectionBuilder(
                allocator, "probeSideIndices", buildSideSingleBatch.getRowCount());
        MarkBuilder markBuilder = getMarkBuilder(buildSideMatched.length)) {
      int buildSideUnmatchedCount = 0;
      for (int i = 0; i < buildSideMatched.length; i++) {
        if (!buildSideMatched[i]) {
          buildSideIndicesBuilder.append(i);
          buildSideUnmatchedCount++;
        }
      }

      try (VectorSchemaRoot probeSideBatch = VectorSchemaRoot.create(probeSideSchema, allocator);
          ArrayDictionaryProvider dictionary =
              ArrayDictionaryProvider.of(allocator, buildSideSingleBatch, probeSideBatch);
          IntVector buildSideIndices = buildSideIndicesBuilder.build(buildSideUnmatchedCount);
          IntVector probeSideIndices = probeSideIndicesBuilder.build(buildSideUnmatchedCount);
          BitVector mark = markBuilder.build(buildSideUnmatchedCount)) {
        output(dictionary, buildSideIndices, probeSideIndices, mark);
      }
    }
  }

  private void output(
      ArrayDictionaryProvider dictionary,
      IntVector buildSideCandidateIndices,
      IntVector proSideCandidateIndices,
      boolean[] probeSideMatched)
      throws ComputeException {

    try (SelectionBuilder buildSideIndicesBuilder =
            new SelectionBuilder(
                allocator, "buildSideIndices", buildSideCandidateIndices.getValueCount());
        SelectionBuilder probeSideIndicesBuilder =
            new SelectionBuilder(
                allocator, "probeSideIndices", proSideCandidateIndices.getValueCount());
        MarkBuilder markBuilder = getMarkBuilder(probeSideMatched.length)) {

      try (VectorSchemaRoot candidate =
              getDictionaryEncodedBatch(
                  buildSideCandidateIndices, proSideCandidateIndices, dictionary);
          BaseIntVector indicesSelection = matcher.filter(allocator, dictionary, candidate, null)) {
        outputMatched(
            buildSideIndicesBuilder,
            probeSideIndicesBuilder,
            markBuilder,
            buildSideCandidateIndices,
            proSideCandidateIndices,
            indicesSelection,
            probeSideMatched);
      }

      if (joinOption.isToOutputProbeSideUnmatched()) {
        outputProbeSideUnmatched(probeSideIndicesBuilder, markBuilder, probeSideMatched);
      }

      try (IntVector probeSideIndices = probeSideIndicesBuilder.build();
          IntVector buildSideIndices =
              buildSideIndicesBuilder.build(probeSideIndices.getValueCount());
          BitVector mark = markBuilder.build(probeSideIndices.getValueCount())) {
        output(dictionary, buildSideIndices, probeSideIndices, mark);
      }
    }
  }

  private MarkBuilder getMarkBuilder(int capacity) {
    if (joinOption.isToOutputMark()) {
      return new MarkBuilder(allocator, "&mark", capacity);
    } else {
      return new MarkBuilder();
    }
  }

  private void outputMatched(
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder,
      IntVector buildSideCandidateIndices,
      IntVector proSideCandidateIndices,
      @Nullable BaseIntVector indicesSelection,
      boolean[] probeSideMatched)
      throws ComputeException {
    Preconditions.checkState(
        buildSideCandidateIndices.getValueCount() == proSideCandidateIndices.getValueCount());
    if (indicesSelection == null) {
      outputMatched(
          buildSideIndicesBuilder,
          probeSideIndicesBuilder,
          markBuilder,
          buildSideCandidateIndices,
          proSideCandidateIndices,
          probeSideMatched,
          proSideCandidateIndices.getValueCount(),
          i -> i);
    } else {
      outputMatched(
          buildSideIndicesBuilder,
          probeSideIndicesBuilder,
          markBuilder,
          buildSideCandidateIndices,
          proSideCandidateIndices,
          probeSideMatched,
          indicesSelection.getValueCount(),
          i -> (int) indicesSelection.getValueAsLong(i));
    }
  }

  private void outputMatched(
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder,
      IntVector buildSideCandidateIndices,
      IntVector proSideCandidateIndices,
      boolean[] probeSideMatched,
      int matchedIndicesCount,
      IntToIntFunction matchedSelectionSupplier)
      throws ComputeException {

    int probeSideMatchedCount = 0;
    for (int i = 0; i < matchedIndicesCount; i++) {
      int selection = matchedSelectionSupplier.apply(i);
      int probeSideMatchedIndex = proSideCandidateIndices.get(selection);

      if (!joinOption.isAllowedToMatchMultiple()) {
        if (probeSideMatched[probeSideMatchedIndex]) {
          throw new ComputeException("the return value of sub-query has more than one rows");
        }
      }
      if (!joinOption.isToOutputAllMatched()) {
        if (probeSideMatched[probeSideMatchedIndex]) {
          continue;
        }
      }

      probeSideMatchedCount++;
      int buildSideMatchedIndex = buildSideCandidateIndices.get(selection);
      buildSideMatched[buildSideMatchedIndex] = true;
      probeSideMatched[probeSideMatchedIndex] = true;
      buildSideIndicesBuilder.append(buildSideMatchedIndex);
      probeSideIndicesBuilder.append(probeSideMatchedIndex);
    }
    markBuilder.appendTrue(probeSideMatchedCount);
  }

  private void outputProbeSideUnmatched(
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder,
      boolean[] probeSideMatched) {
    int probeSideUnmatchedCount = 0;
    for (int probeSideIndex = 0; probeSideIndex < probeSideMatched.length; probeSideIndex++) {
      if (!probeSideMatched[probeSideIndex]) {
        probeSideUnmatchedCount++;
        probeSideIndicesBuilder.append(probeSideIndex);
      }
    }
    markBuilder.appendFalse(probeSideUnmatchedCount);
  }

  private VectorSchemaRoot getDictionaryEncodedBatch(
      IntVector buildSideCandidateIndices,
      IntVector probeSideIndices,
      ArrayDictionaryProvider dictionaryProvider) {
    List<FieldVector> vectors = new ArrayList<>();
    int buildSideColumnCount = buildSideSingleBatch.getFieldVectors().size();
    for (int i = 0; i < buildSideColumnCount; i++) {
      vectors.add(
          ValueVectors.slice(allocator, buildSideCandidateIndices, dictionaryProvider.lookup(i)));
    }
    int probeSideColumnCount = probeSideSchema.getFields().size();
    for (int i = 0; i < probeSideColumnCount; i++) {
      vectors.add(
          ValueVectors.slice(
              allocator, probeSideIndices, dictionaryProvider.lookup(i + buildSideColumnCount)));
    }
    return VectorSchemaRoots.create(vectors, probeSideIndices.getValueCount());
  }

  private void output(
      ArrayDictionaryProvider dictionaryProvider,
      BaseIntVector buildSideIndices,
      BaseIntVector probeSideIndices,
      @Nullable BitVector mark)
      throws ComputeException {
    Preconditions.checkArgument(
        buildSideIndices.getValueCount() == probeSideIndices.getValueCount());

    if (mark != null) {
      Preconditions.checkArgument(buildSideIndices.getValueCount() == mark.getValueCount());
    }
    int buildSideColumnCount = buildSideSingleBatch.getFieldVectors().size();
    int probeSideColumnCount = probeSideSchema.getFields().size();

    List<FieldVector> vectors = new ArrayList<>();
    for (int i = 0; i < buildSideColumnCount; i++) {
      vectors.add(ValueVectors.slice(allocator, buildSideIndices, dictionaryProvider.lookup(i)));
    }
    for (int i = 0; i < probeSideColumnCount; i++) {
      vectors.add(
          ValueVectors.slice(
              allocator, probeSideIndices, dictionaryProvider.lookup(i + buildSideColumnCount)));
    }
    if (mark != null) {
      vectors.add(ValueVectors.slice(allocator, mark));
    }

    try (VectorSchemaRoot result =
            VectorSchemaRoots.create(vectors, probeSideIndices.getValueCount());
        VectorSchemaRoot output =
            ScalarExpressions.evaluate(
                allocator, dictionaryProvider, result, null, outputExpressions);
        BaseIntVector selection = getSelection(probeSideIndices)) {
      resultConsumer.consume(
          dictionaryProvider.slice(allocator),
          VectorSchemaRoots.transfer(allocator, output),
          ValueVectors.slice(allocator, selection));
    }
  }

  @Nullable
  private BaseIntVector getSelection(BaseIntVector probeSideIndices) {
    if (!joinOption.isToOutputProbeSideUnmatched() || !joinOption.isOrderByProbeSideOrdinal()) {
      return null;
    }
    if (isInOrder(probeSideIndices)) {
      return null;
    }

    IntVector selection = new IntVector("selection", allocator);
    selection.allocateNew(probeSideIndices.getValueCount());
    selection.setValueCount(probeSideIndices.getValueCount());
    new IndexSorter<>()
        .sort(
            probeSideIndices,
            selection,
            new StableVectorComparator<>(
                DefaultVectorComparators.createDefaultComparator(probeSideIndices)));
    return selection;
  }

  private static boolean isInOrder(BaseIntVector vector) {
    for (int i = 1; i < vector.getValueCount(); i++) {
      if (vector.getValueAsLong(i) < vector.getValueAsLong((i - 1))) {
        return false;
      }
    }
    return true;
  }

  public static class Builder implements AutoCloseable {

    private final BufferAllocator allocator;
    private final ScalarExpression<IntVector> buildSideHasher;
    private final Schema buildSideSchema;
    private final List<VectorSchemaRoot> buildSideBatches;

    public Builder(
        BufferAllocator allocator,
        ScalarExpression<IntVector> buildSideHasher,
        Schema buildSideSchema) {
      this.allocator = Objects.requireNonNull(allocator);
      this.buildSideHasher = Objects.requireNonNull(buildSideHasher);
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.buildSideBatches = new ArrayList<>();
    }

    public Builder add(@WillClose VectorSchemaRoot buildSideBatch) throws ComputeException {
      buildSideBatches.add(buildSideBatch);
      return this;
    }

    @Override
    public void close() {
      buildSideBatches.forEach(VectorSchemaRoot::close);
      buildSideBatches.clear();
    }

    public JoinHashMap build(
        BufferAllocator allocator,
        JoinOption joinOption,
        PredicateExpression matcher,
        List<ScalarExpression<?>> outputExpressions,
        ScalarExpression<IntVector> probeSideHasher,
        Schema probeSideSchema,
        ResultConsumer resultConsumer)
        throws ComputeException {
      Preconditions.checkNotNull(allocator);
      Preconditions.checkNotNull(joinOption);
      Preconditions.checkNotNull(matcher);
      Preconditions.checkNotNull(outputExpressions);
      Preconditions.checkNotNull(probeSideHasher);
      Preconditions.checkNotNull(probeSideSchema);
      Preconditions.checkNotNull(resultConsumer);

      IntObjectHashMap<List<Integer>> buildSideMap = new IntObjectHashMap<>();

      try (VectorSchemaRoot buildSideSingleBatch =
          VectorSchemaRoots.concat(allocator, buildSideSchema, buildSideBatches)) {
        try (IntVector buildSideHashCodes =
            buildSideHasher.invoke(allocator, buildSideSingleBatch)) {
          for (int i = 0; i < buildSideHashCodes.getValueCount(); i++) {
            int hashCode = buildSideHashCodes.get(i);
            buildSideMap.computeIfAbsent(hashCode, k -> new ArrayList<>()).add(i);
          }
        }

        return new JoinHashMap(
            allocator,
            joinOption,
            matcher,
            outputExpressions,
            probeSideHasher,
            probeSideSchema,
            buildSideMap,
            VectorSchemaRoots.transfer(allocator, buildSideSingleBatch),
            resultConsumer);
      }
    }
  }
}
