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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
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
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinArrayList extends CrossJoinArrayList {

  protected final JoinOption joinOption;
  protected final PredicateExpression matcher;
  protected final boolean[] buildSideMatched;
  protected final boolean[] unusedByMatcherInBuildSide;
  protected final boolean[] unusedByMatcherInProbeSide;

  JoinArrayList(CrossJoinArrayList parent, JoinOption joinOption, PredicateExpression matcher) {
    super(parent);
    this.joinOption = joinOption;
    this.matcher = matcher;
    this.buildSideMatched = new boolean[buildSideSingleBatch.getRowCount()];
    this.unusedByMatcherInBuildSide = new boolean[buildSideSingleBatch.getFieldVectors().size()];
    this.unusedByMatcherInProbeSide = new boolean[probeSideSchema.getFields().size()];

    boolean[] usedByMatcher =
        new boolean
            [buildSideSingleBatch.getFieldVectors().size() + probeSideSchema.getFields().size()];
    matcher.getLeafExpressions().stream()
        .filter(e -> e instanceof FieldNode)
        .map(e -> (FieldNode) e)
        .mapToInt(FieldNode::getIndex)
        .forEach(index -> usedByMatcher[index] = true);
    for (int i = 0; i < unusedByMatcherInBuildSide.length; i++) {
      unusedByMatcherInBuildSide[i] = !usedByMatcher[i];
    }
    for (int i = 0; i < unusedByMatcherInProbeSide.length; i++) {
      unusedByMatcherInProbeSide[i] = !usedByMatcher[i + unusedByMatcherInBuildSide.length];
    }
  }

  JoinArrayList(JoinArrayList o) {
    this(o, o.joinOption, o.matcher);
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

      buildSideIndicesBuilder.setNullable(joinOption.isToOutputProbeSideUnmatched());
      probeSideIndicesBuilder.setNullable(joinOption.isToOutputBuildSideUnmatched());
      markBuilder.setNullable(joinOption.isToOutputBuildSideUnmatched());

      try (VectorSchemaRoot probeSideBatch = VectorSchemaRoot.create(probeSideSchema, allocator);
          IntVector buildSideIndices = buildSideIndicesBuilder.build(buildSideUnmatchedCount);
          IntVector probeSideIndices = probeSideIndicesBuilder.build(buildSideUnmatchedCount);
          BitVector mark = markBuilder.build(buildSideUnmatchedCount)) {
        output(
            ArrowDictionaries.emptyProvider(),
            probeSideBatch,
            buildSideIndices,
            probeSideIndices,
            mark,
            null);
      }
    }
  }

  @Override
  protected MarkBuilder getMarkBuilder(int capacity) {
    if (joinOption.isToOutputMark()) {
      return new MarkBuilder(allocator, "&mark", capacity);
    } else {
      return new MarkBuilder();
    }
  }

  @Override
  protected int outputMatchedAndProbeSideUnmatched(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder)
      throws ComputeException {
    boolean[] probeSideMatched = new boolean[probeSideBatch.getRowCount()];

    int matchedCount =
        outputMatchedBeforeBuildCandidates(
            probeSideDictionaryProvider,
            probeSideBatch,
            buildSideIndicesBuilder,
            probeSideIndicesBuilder,
            probeSideMatched);

    int unmatchedCount = 0;
    if (joinOption.isToOutputProbeSideUnmatched()) {
      unmatchedCount = outputProbeSideUnmatched(probeSideIndicesBuilder, probeSideMatched);
    }

    markBuilder.appendTrue(matchedCount);
    markBuilder.appendFalse(unmatchedCount);

    buildSideIndicesBuilder.setNullable(joinOption.isToOutputProbeSideUnmatched());
    probeSideIndicesBuilder.setNullable(joinOption.isToOutputBuildSideUnmatched());
    markBuilder.setNullable(joinOption.isToOutputBuildSideUnmatched());

    return unmatchedCount;
  }

  protected int outputMatchedBeforeBuildCandidates(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      boolean[] probeSideMatched)
      throws ComputeException {
    int matchedCount = 0;
    for (int buildSideIndex = 0;
        buildSideIndex < buildSideSingleBatch.getRowCount();
        buildSideIndex++) {
      try (IntVector buildSideCandidateIndices =
          ConstantVectors.of(allocator, buildSideIndex, probeSideMatched.length)) {
        matchedCount +=
            outputMatchedBeforeFilter(
                probeSideDictionaryProvider,
                probeSideBatch,
                buildSideIndicesBuilder,
                probeSideIndicesBuilder,
                buildSideCandidateIndices,
                null,
                probeSideMatched);
      }
    }
    return matchedCount;
  }

  protected int outputMatchedBeforeFilter(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      BaseIntVector buildSideCandidateIndices,
      @Nullable BaseIntVector proSideCandidateIndices,
      boolean[] probeSideMatched)
      throws ComputeException {
    try (LazyBatch selectedBuildSideAndProbeSide =
            selectBuildSideAndProbeSide(
                probeSideDictionaryProvider,
                probeSideBatch,
                buildSideCandidateIndices,
                proSideCandidateIndices,
                unusedByMatcherInBuildSide,
                unusedByMatcherInProbeSide);
        BaseIntVector indicesSelection =
            matcher.filter(
                allocator,
                selectedBuildSideAndProbeSide.getDictionaryProvider(),
                selectedBuildSideAndProbeSide.getData(),
                null)) {
      return outputMatchedAfterFilter(
          buildSideIndicesBuilder,
          probeSideIndicesBuilder,
          buildSideCandidateIndices,
          proSideCandidateIndices,
          indicesSelection,
          probeSideMatched);
    }
  }

  private int outputMatchedAfterFilter(
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

      final int buildSideMatchedIndex = (int) buildSideCandidateIndices.getValueAsLong(selection);

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

  @Nullable
  @Override
  protected BaseIntVector getSelection(BaseIntVector probeSideIndices, int unmatchedCount) {
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
      while (leftCursor < matchedCount && rightCursor < total) {
        if (isInOrder(probeSideIndices, leftCursor, rightCursor)) {
          selectionBuilder.append(leftCursor++);
        } else {
          selectionBuilder.append(rightCursor++);
        }
      }
      while (leftCursor < matchedCount) {
        selectionBuilder.append(leftCursor++);
      }
      while (rightCursor < total) {
        selectionBuilder.append(rightCursor++);
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

  public static class Builder extends CrossJoinArrayList.Builder {
    private final JoinOption joinOption;
    private final PredicateExpression matcher;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        Schema probeSideSchema,
        List<ScalarExpression<?>> outputExpressions,
        JoinOption joinOption,
        PredicateExpression matcher) {
      super(allocator, buildSideSchema, probeSideSchema, outputExpressions);
      this.joinOption = Objects.requireNonNull(joinOption);
      this.matcher = Objects.requireNonNull(matcher);
    }

    @Override
    public Schema constructOutputSchema() throws ComputeException {
      List<Field> outputFields = new ArrayList<>();
      boolean buildSideMustNullable = joinOption.isToOutputProbeSideUnmatched();
      boolean probeSideMustNullable = joinOption.isToOutputBuildSideUnmatched();
      buildSideSchema.getFields().stream()
          .map(
              field ->
                  Schemas.fieldWithNullable(field, buildSideMustNullable || field.isNullable()))
          .forEach(outputFields::add);
      probeSideSchema.getFields().stream()
          .map(
              field ->
                  Schemas.fieldWithNullable(field, probeSideMustNullable || field.isNullable()))
          .forEach(outputFields::add);
      outputFields.add(
          new Field(
              "mark",
              new FieldType(probeSideMustNullable, Types.MinorType.BIT.getType(), null),
              null));
      return ScalarExpressionUtils.getOutputSchema(
          allocator, outputExpressions, new Schema(outputFields));
    }

    @Override
    public JoinArrayList build(ResultConsumer resultConsumer) throws ComputeException {
      try (CrossJoinArrayList crossJoinArrayList = super.build(resultConsumer)) {
        return new JoinArrayList(crossJoinArrayList, joinOption, matcher);
      }
    }
  }
}
