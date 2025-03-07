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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinArrayList implements JoinCollection {

  private final BufferAllocator allocator;
  private final JoinOption joinOption;
  private final PredicateExpression matcher;
  private final List<ScalarExpression<?>> outputExpressions;
  private final Schema probeSideSchema;
  private final VectorSchemaRoot buildSideSingleBatch;
  private final boolean[] buildSideMatched;
  private final ResultConsumer resultConsumer;

  public JoinArrayList(
      BufferAllocator allocator,
      JoinOption joinOption,
      PredicateExpression matcher,
      List<ScalarExpression<?>> outputExpressions,
      Schema probeSideSchema,
      VectorSchemaRoot buildSideSingleBatch,
      ResultConsumer resultConsumer) {
    this.allocator = allocator;
    this.joinOption = joinOption;
    this.matcher = matcher;
    this.outputExpressions = outputExpressions;
    this.probeSideSchema = probeSideSchema;
    this.buildSideSingleBatch = buildSideSingleBatch;
    this.buildSideMatched = new boolean[buildSideSingleBatch.getRowCount()];
    this.resultConsumer = resultConsumer;
  }

  @Override
  public void close() {
    buildSideSingleBatch.close();
  }

  @Override
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
        output(dictionary, buildSideIndices, probeSideIndices, mark, 0);
      }
    }
  }

  @Override
  public void probe(
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    try (VectorSchemaRoot flattened =
        VectorSchemaRoots.flatten(allocator, dictionaryProvider, data, selection)) {
      probe(flattened);
    }
  }

  public void probe(VectorSchemaRoot probeSideBatch) throws ComputeException {
    Preconditions.checkArgument(probeSideBatch.getSchema().equals(probeSideSchema));

    try (ArrayDictionaryProvider dictionary =
            ArrayDictionaryProvider.of(allocator, buildSideSingleBatch, probeSideBatch);
        SelectionBuilder buildSideIndicesBuilder =
            new SelectionBuilder(allocator, "buildSideIndices", probeSideBatch.getRowCount());
        SelectionBuilder probeSideIndicesBuilder =
            new SelectionBuilder(allocator, "probeSideIndices", probeSideBatch.getRowCount());
        MarkBuilder markBuilder = getMarkBuilder(probeSideBatch.getRowCount())) {
      boolean[] probeSideMatched = new boolean[probeSideBatch.getRowCount()];

      int matchedCount = 0;
      for (int buildSideIndex = 0;
          buildSideIndex < buildSideSingleBatch.getRowCount();
          buildSideIndex++) {
        try (SelectionBuilder buildSideCandidateIndicesBuilder =
            new SelectionBuilder(allocator, "tempBuildSideIndices", probeSideBatch.getRowCount())) {
          for (int probeSideIndex = 0;
              probeSideIndex < probeSideBatch.getRowCount();
              probeSideIndex++) {
            buildSideCandidateIndicesBuilder.append(buildSideIndex);
          }

          try (IntVector buildSideCandidateIndices = buildSideCandidateIndicesBuilder.build()) {
            try (VectorSchemaRoot candidate =
                    getDictionaryEncodedBatch(buildSideCandidateIndices, null, dictionary);
                BaseIntVector indicesSelection =
                    matcher.filter(allocator, dictionary, candidate, null)) {
              matchedCount +=
                  outputMatched(
                      buildSideIndicesBuilder,
                      probeSideIndicesBuilder,
                      buildSideCandidateIndices,
                      null,
                      indicesSelection,
                      probeSideMatched);
            }
          }
        }
      }

      int unmatchedCount = 0;
      if (joinOption.isToOutputProbeSideUnmatched()) {
        unmatchedCount = outputProbeSideUnmatched(probeSideIndicesBuilder, probeSideMatched);
      }

      markBuilder.appendTrue(matchedCount);
      markBuilder.appendFalse(unmatchedCount);

      try (IntVector probeSideIndices = probeSideIndicesBuilder.build();
          IntVector buildSideIndices =
              buildSideIndicesBuilder.build(probeSideIndices.getValueCount());
          BitVector mark = markBuilder.build(probeSideIndices.getValueCount())) {
        output(dictionary, buildSideIndices, probeSideIndices, mark, unmatchedCount);
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

  private int outputMatched(
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      BaseIntVector buildSideCandidateIndices,
      @Nullable BaseIntVector proSideCandidateIndices,
      @Nullable BaseIntVector matchedIndices,
      boolean[] probeSideMatched)
      throws ComputeException {
    int probeSideMatchedCount = 0;
    final int outputRowCount =
        matchedIndices != null
            ? matchedIndices.getValueCount()
            : buildSideCandidateIndices.getValueCount();
    for (int i = 0; i < outputRowCount; i++) {
      final int selection;
      if (matchedIndices == null) {
        selection = i;
      } else {
        selection = (int) matchedIndices.getValueAsLong(i);
      }

      final int buildSideMatchedIndex = (int) buildSideCandidateIndices.getValueAsLong(i);

      final int probeSideMatchedIndex;
      if (proSideCandidateIndices == null) {
        probeSideMatchedIndex = selection;
      } else {
        probeSideMatchedIndex = (int) proSideCandidateIndices.getValueAsLong(selection);
      }

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
      buildSideMatched[buildSideMatchedIndex] = true;
      probeSideMatched[probeSideMatchedIndex] = true;
      buildSideIndicesBuilder.append(buildSideMatchedIndex);
      probeSideIndicesBuilder.append(probeSideMatchedIndex);
    }
    return probeSideMatchedCount;
  }

  private int outputProbeSideUnmatched(
      SelectionBuilder probeSideIndicesBuilder, boolean[] probeSideMatched) {
    int probeSideUnmatchedCount = 0;
    for (int probeSideIndex = 0; probeSideIndex < probeSideMatched.length; probeSideIndex++) {
      if (!probeSideMatched[probeSideIndex]) {
        probeSideUnmatchedCount++;
        probeSideIndicesBuilder.append(probeSideIndex);
      }
    }
    return probeSideUnmatchedCount;
  }

  protected VectorSchemaRoot getDictionaryEncodedBatch(
      IntVector buildSideCandidateIndices,
      @Nullable IntVector probeSideCandidateIndices,
      ArrayDictionaryProvider dictionaryProvider) {
    int buildSideColumnCount = buildSideSingleBatch.getFieldVectors().size();
    int probeSideColumnCount = probeSideSchema.getFields().size();
    List<FieldVector> vectors = new ArrayList<>();
    for (int i = 0; i < buildSideColumnCount; i++) {
      vectors.add(
          ValueVectors.slice(allocator, buildSideCandidateIndices, dictionaryProvider.lookup(i)));
    }
    for (int i = 0; i < probeSideColumnCount; i++) {
      vectors.add(
          ValueVectors.slice(
              allocator,
              probeSideCandidateIndices,
              dictionaryProvider.lookup(i + buildSideColumnCount)));
    }
    return new VectorSchemaRoot(vectors);
  }

  private void output(
      ArrayDictionaryProvider dictionaryProvider,
      BaseIntVector buildSideIndices,
      BaseIntVector probeSideIndices,
      @Nullable BitVector mark,
      int unmatchedCount)
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
            ScalarExpressionUtils.evaluate(
                allocator, dictionaryProvider, result, null, outputExpressions);
        BaseIntVector selection = getSelection(probeSideIndices, unmatchedCount)) {
      resultConsumer.consume(dictionaryProvider, output, selection);
    }
  }

  @Nullable
  private BaseIntVector getSelection(BaseIntVector probeSideIndices, int unmatchedCount) {
    int total = probeSideIndices.getValueCount();
    int matchedCount = total - unmatchedCount;
    if (matchedCount == 0 || unmatchedCount == 0 || !joinOption.isOrderByProbeSideOrdinal()) {
      return null;
    }

    if (isInOrder(probeSideIndices, matchedCount - 1, matchedCount)) {
      return null;
    }

    try (SelectionBuilder selectionBuilder = new SelectionBuilder(allocator, "selection", total)) {
      // merge from [0,matchedCount) and from [matchedCount,probeSideIndices.getValueCount())
      int leftCursor = 0;
      int rightCursor = matchedCount;
      while (true) {
        if (leftCursor < matchedCount) {
          if (rightCursor < total) {
            if (isInOrder(probeSideIndices, leftCursor, rightCursor)) {
              selectionBuilder.append(leftCursor++);
            } else {
              selectionBuilder.append(rightCursor++);
            }
          } else {
            selectionBuilder.append(leftCursor++);
          }
        } else {
          if (rightCursor < total) {
            selectionBuilder.append(rightCursor++);
          } else {
            break;
          }
        }
      }
      return selectionBuilder.build();
    }
  }

  private static boolean isInOrder(BaseIntVector vector, int leftIndex, int rightIndex) {
    // null is biggest
    if (vector.getField().isNullable()) {
      if (vector.isNull(rightIndex)) {
        return true;
      }
      if (vector.isNull(leftIndex)) {
        return false;
      }
    }
    return vector.getValueAsLong(leftIndex) <= vector.getValueAsLong(rightIndex);
  }

  public static class Builder implements JoinCollection.Builder<Builder> {

    private final BufferAllocator allocator;
    private final Schema buildSideSchema;
    private final Schema probeSideSchema;
    private final List<ScalarExpression<?>> outputExpressions;
    private final JoinOption joinOption;
    private final PredicateExpression matcher;

    private final List<VectorSchemaRoot> buildSideBatches;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        Schema probeSideSchema,
        List<ScalarExpression<?>> outputExpressions,
        JoinOption joinOption,
        PredicateExpression matcher) {
      this.allocator = Objects.requireNonNull(allocator);
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.probeSideSchema = Objects.requireNonNull(probeSideSchema);
      this.outputExpressions = Objects.requireNonNull(outputExpressions);
      this.joinOption = Objects.requireNonNull(joinOption);
      this.matcher = Objects.requireNonNull(matcher);

      this.buildSideBatches = new ArrayList<>();
    }

    @Override
    public Schema constructOutputSchema() throws ComputeException {
      List<Field> outputFields = new ArrayList<>();
      outputFields.addAll(buildSideSchema.getFields());
      outputFields.addAll(probeSideSchema.getFields());
      outputFields.add(Field.nullable("mark", Types.MinorType.BIT.getType()));
      return ScalarExpressionUtils.getOutputSchema(
          allocator, outputExpressions, new Schema(outputFields));
    }

    @Override
    public void close() {
      buildSideBatches.forEach(VectorSchemaRoot::close);
      buildSideBatches.clear();
    }

    @Override
    public JoinArrayList.Builder add(
        DictionaryProvider dictionaryProvider,
        VectorSchemaRoot data,
        @Nullable BaseIntVector selection)
        throws ComputeException {
      VectorSchemaRoot flattened =
          VectorSchemaRoots.flatten(allocator, dictionaryProvider, data, selection);
      buildSideBatches.add(flattened);
      return this;
    }

    @Override
    public JoinCollection build(ResultConsumer resultConsumer) throws ComputeException {
      Objects.requireNonNull(resultConsumer);

      try (VectorSchemaRoot buildSideSingleBatch =
          VectorSchemaRoots.concat(allocator, buildSideSchema, buildSideBatches)) {

        return new JoinArrayList(
            allocator,
            joinOption,
            matcher,
            outputExpressions,
            probeSideSchema,
            VectorSchemaRoots.transfer(allocator, buildSideSingleBatch),
            resultConsumer);
      }
    }
  }
}
