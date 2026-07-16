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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorAppender;
import org.apache.commons.lang3.tuple.Pair;

public class CrossJoinArrayList implements JoinCollection {

  protected final BufferAllocator allocator;
  protected final List<ScalarExpression<?>> outputExpressions;
  protected final Schema probeSideSchema;
  protected final VectorSchemaRoot buildSideSingleBatch;
  protected final MapDictionaryProvider buildSideDictionaryProvider;
  protected final ResultConsumer resultConsumer;

  CrossJoinArrayList(
      BufferAllocator allocator,
      List<ScalarExpression<?>> outputExpressions,
      Schema probeSideSchema,
      VectorSchemaRoot buildSideSingleBatch,
      DictionaryProvider buildSideDictionaryProvider,
      ResultConsumer resultConsumer) {
    this.allocator = allocator;
    this.outputExpressions = outputExpressions;
    this.probeSideSchema = probeSideSchema;
    this.buildSideSingleBatch = VectorSchemaRoots.slice(allocator, buildSideSingleBatch);
    this.buildSideDictionaryProvider =
        ArrowDictionaries.slice(
            allocator, buildSideDictionaryProvider, buildSideSingleBatch.getSchema());
    this.resultConsumer = resultConsumer;
  }

  CrossJoinArrayList(CrossJoinArrayList o) {
    this(
        o.allocator,
        o.outputExpressions,
        o.probeSideSchema,
        o.buildSideSingleBatch,
        o.buildSideDictionaryProvider,
        o.resultConsumer);
  }

  @Override
  public void close() {
    buildSideSingleBatch.close();
    buildSideDictionaryProvider.close();
  }

  @Override
  public void flush() throws ComputeException {}

  @Override
  public void probe(DictionaryProvider probeSideDictionaryProvider, VectorSchemaRoot probeSideBatch)
      throws ComputeException {
    Preconditions.checkArgument(
        probeSideSchema.equals(
            ArrowDictionaries.flatten(probeSideDictionaryProvider, probeSideBatch.getSchema())));

    // TODO: cross join 的扫描侧不需要  probeSideIndices，可以进行优化
    try (SelectionBuilder probeSideIndicesBuilder =
            new SelectionBuilder(allocator, "tempProbeSideIndices", probeSideBatch.getRowCount());
        SelectionBuilder buildSideIndicesBuilder =
            new SelectionBuilder(allocator, "tempBuildSideIndices", probeSideBatch.getRowCount());
        MarkBuilder markBuilder = getMarkBuilder(probeSideBatch.getRowCount())) {

      int unmatchedCount =
          outputMatchedAndProbeSideUnmatched(
              probeSideDictionaryProvider,
              probeSideBatch,
              buildSideIndicesBuilder,
              probeSideIndicesBuilder,
              markBuilder);

      try (IntVector probeSideIndices = probeSideIndicesBuilder.build();
          IntVector buildSideIndices =
              buildSideIndicesBuilder.build(probeSideIndices.getValueCount());
          BitVector mark = markBuilder.build(probeSideIndices.getValueCount());
          BaseIntVector selection = getSelection(probeSideIndices, unmatchedCount)) {
        output(
            probeSideDictionaryProvider,
            probeSideBatch,
            buildSideIndices,
            probeSideIndices,
            mark,
            selection);
      }
    }
  }

  protected MarkBuilder getMarkBuilder(int rowCount) {
    return new MarkBuilder();
  }

  protected int outputMatchedAndProbeSideUnmatched(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder)
      throws ComputeException {
    int probeSideBatchRowCount = probeSideBatch.getRowCount();
    for (int buildSideIndex = 0;
        buildSideIndex < buildSideSingleBatch.getRowCount();
        buildSideIndex++) {
      for (int probeSideIndex = 0; probeSideIndex < probeSideBatchRowCount; probeSideIndex++) {
        buildSideIndicesBuilder.append(buildSideIndex);
        probeSideIndicesBuilder.append(probeSideIndex);
      }
    }
    return 0;
  }

  protected BaseIntVector getSelection(BaseIntVector probeSideIndices, int unmatchedCount) {
    return null;
  }

  protected void output(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      BaseIntVector buildSideIndices,
      @Nullable BaseIntVector probeSideIndices,
      @Nullable BitVector mark,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    if (probeSideIndices != null) {
      Preconditions.checkArgument(
          buildSideIndices.getValueCount() == probeSideIndices.getValueCount());
    }
    if (mark != null) {
      Preconditions.checkArgument(buildSideIndices.getValueCount() == mark.getValueCount());
    }

    if (selection != null) {
      try (BaseIntVector selectedBuildSideIndices =
              ValueVectors.select(allocator, buildSideIndices, selection);
          BaseIntVector selectedProbeSideIndices =
              probeSideIndices == null
                  ? ValueVectors.slice(allocator, selection)
                  : ValueVectors.select(allocator, probeSideIndices, selection);
          BitVector selectedMark = ValueVectors.select(allocator, mark, selection)) {
        output(
            probeSideDictionaryProvider,
            probeSideBatch,
            selectedBuildSideIndices,
            selectedProbeSideIndices,
            selectedMark,
            null);
        return;
      }
    }

    try (LazyBatch buildSideAndProbeSideSelected =
        selectBuildSideAndProbeSide(
            probeSideDictionaryProvider,
            probeSideBatch,
            buildSideIndices,
            probeSideIndices,
            null,
            null)) {

      List<FieldVector> vectors = new ArrayList<>();
      for (FieldVector vector : buildSideAndProbeSideSelected.getData().getFieldVectors()) {
        vectors.add(ValueVectors.slice(allocator, vector));
      }
      if (mark != null) {
        vectors.add(ValueVectors.slice(allocator, mark));
      }

      try (VectorSchemaRoot result =
              VectorSchemaRoots.create(vectors, buildSideIndices.getValueCount());
          VectorSchemaRoot output =
              ScalarExpressionUtils.evaluate(
                  allocator,
                  buildSideAndProbeSideSelected.getDictionaryProvider(),
                  result,
                  null,
                  outputExpressions)) {
        resultConsumer.consume(buildSideAndProbeSideSelected.getDictionaryProvider(), output);
      }
    }
  }

  protected LazyBatch selectBuildSideAndProbeSide(
      DictionaryProvider probeSideDictionaryProvider,
      VectorSchemaRoot probeSideBatch,
      BaseIntVector buildSideIndices,
      @Nullable BaseIntVector probeSideIndices,
      @Nullable boolean[] unusedInBuildSide,
      @Nullable boolean[] unusedInProbeSide) {
    try (LazyBatch buildSideSelected =
            ArrowDictionaries.select(
                allocator,
                buildSideDictionaryProvider,
                buildSideSingleBatch,
                buildSideIndices,
                unusedInBuildSide);
        LazyBatch probeSideSelected =
            ArrowDictionaries.select(
                allocator,
                probeSideDictionaryProvider,
                probeSideBatch,
                probeSideIndices,
                unusedInProbeSide)) {
      return ArrowDictionaries.join(allocator, buildSideSelected, probeSideSelected);
    }
  }

  public static class Builder implements JoinCollection.Builder {

    protected final BufferAllocator allocator;
    protected final Schema buildSideSchema;
    protected final Schema probeSideSchema;
    protected final List<ScalarExpression<?>> outputExpressions;
    protected final List<LazyBatch> buildSideBatches;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        Schema probeSideSchema,
        List<ScalarExpression<?>> outputExpressions) {
      this.allocator = Objects.requireNonNull(allocator);
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.probeSideSchema = Objects.requireNonNull(probeSideSchema);
      this.outputExpressions = Objects.requireNonNull(outputExpressions);
      this.buildSideBatches = new ArrayList<>();
    }

    @Override
    public Schema constructOutputSchema() throws ComputeException {
      List<Field> outputFields = new ArrayList<>();
      outputFields.addAll(buildSideSchema.getFields());
      outputFields.addAll(probeSideSchema.getFields());
      return ScalarExpressionUtils.getOutputSchema(
          allocator, outputExpressions, new Schema(outputFields));
    }

    @Override
    public void close() {
      buildSideBatches.forEach(LazyBatch::close);
      buildSideBatches.clear();
    }

    @Override
    public void add(DictionaryProvider dictionaryProvider, VectorSchemaRoot data)
        throws ComputeException {
      Preconditions.checkArgument(
          buildSideSchema.equals(ArrowDictionaries.flatten(dictionaryProvider, data.getSchema())));
      buildSideBatches.add(LazyBatch.slice(allocator, dictionaryProvider, data));
    }

    @Override
    public CrossJoinArrayList build(ResultConsumer resultConsumer) throws ComputeException {
      Objects.requireNonNull(resultConsumer);

      if (buildSideBatches.isEmpty()) {
        try (VectorSchemaRoot buildSideSingleBatch =
            VectorSchemaRoot.create(buildSideSchema, allocator)) {
          return new CrossJoinArrayList(
              allocator,
              outputExpressions,
              probeSideSchema,
              buildSideSingleBatch,
              ArrowDictionaries.emptyProvider(),
              resultConsumer);
        }
      }

      int rowCount =
          buildSideBatches.stream()
              .map(LazyBatch::getData)
              .mapToInt(VectorSchemaRoot::getRowCount)
              .sum();
      List<FieldVector> resultVectors = new ArrayList<>();
      for (int i = 0; i < buildSideSchema.getFields().size(); i++) {
        List<Pair<FieldVector, Dictionary>> dictionaryColumns = getColumn(i);

        // 没有字典的列 || 每个 batch 的字典相同: 拷贝时不考虑字典
        if (dictionaryColumns.stream().map(Pair::getRight).anyMatch(Objects::isNull)
            || dictionaryColumns.stream()
                .map(Pair::getRight)
                .allMatch(d -> isSameDictionary(d, dictionaryColumns.get(0).getRight()))) {
          FieldVector dest = dictionaryColumns.get(0).getLeft().getField().createVector(allocator);
          dest.allocateNew();
          VectorAppender appender = new VectorAppender(dest);
          dictionaryColumns.stream()
              .map(Pair::getLeft)
              .forEach(vector -> vector.accept(appender, null));
          resultVectors.add(dest);
          continue;
        }

        // 默认进行物化
        FieldVector dest = buildSideSchema.getFields().get(i).createVector(allocator);
        dest.allocateNew();
        VectorAppender appender = new VectorAppender(dest);
        for (Pair<FieldVector, Dictionary> pair : dictionaryColumns) {
          FieldVector vector = pair.getLeft();
          Dictionary dictionary = pair.getRight();
          try (FieldVector flattened =
              ArrowDictionaries.flatten(allocator, dictionary, vector, null)) {
            flattened.accept(appender, null);
          }
        }
        resultVectors.add(dest);
      }

      try (VectorSchemaRoot buildSideSingleBatch =
          VectorSchemaRoots.create(resultVectors, rowCount)) {
        return new CrossJoinArrayList(
            allocator,
            outputExpressions,
            probeSideSchema,
            buildSideSingleBatch,
            buildSideBatches.get(0).getDictionaryProvider(),
            resultConsumer);
      }
    }

    private List<Pair<FieldVector, Dictionary>> getColumn(int index) {
      List<Pair<FieldVector, Dictionary>> dictionaryColumns = new ArrayList<>();
      for (LazyBatch buildSideBatch : buildSideBatches) {
        FieldVector vector = buildSideBatch.getData().getVector(index);
        DictionaryProvider dictionaryProvider = buildSideBatch.getDictionaryProvider();
        if (vector.getField().getDictionary() == null) {
          dictionaryColumns.add(Pair.of(vector, null));
        } else {
          Dictionary dictionary =
              dictionaryProvider.lookup(vector.getField().getDictionary().getId());
          dictionaryColumns.add(Pair.of(vector, dictionary));
        }
      }
      return dictionaryColumns;
    }

    private static boolean isSameDictionary(Dictionary d1, Dictionary d2) {
      if (d1 == null || d2 == null) {
        return false;
      }
      if (!d1.getEncoding().equals(d2.getEncoding())) {
        return false;
      }
      if (!d1.getVectorType().equals(d1.getVectorType())) {
        return false;
      }
      FieldVector v1 = d1.getVector();
      FieldVector v2 = d2.getVector();
      if (v1.getValueCount() == 0 || v2.getValueCount() == 0) {
        return true;
      }
      long[] v1BufferAddresses =
          Arrays.stream(v1.getBuffers(false)).mapToLong(ArrowBuf::memoryAddress).toArray();
      long[] v2BufferAddresses =
          Arrays.stream(v2.getBuffers(false)).mapToLong(ArrowBuf::memoryAddress).toArray();
      return Arrays.equals(v1BufferAddresses, v2BufferAddresses);
    }
  }
}
