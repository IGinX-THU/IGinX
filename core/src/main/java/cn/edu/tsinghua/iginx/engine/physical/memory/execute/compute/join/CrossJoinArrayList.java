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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CrossJoinArrayList implements JoinCollection {

  protected final BufferAllocator allocator;
  protected final List<ScalarExpression<?>> outputExpressions;
  protected final Schema probeSideSchema;
  protected final VectorSchemaRoot buildSideSingleBatch;
  protected final ResultConsumer resultConsumer;

  CrossJoinArrayList(
      BufferAllocator allocator,
      List<ScalarExpression<?>> outputExpressions,
      Schema probeSideSchema,
      VectorSchemaRoot buildSideSingleBatch,
      ResultConsumer resultConsumer) {
    this.allocator = allocator;
    this.outputExpressions = outputExpressions;
    this.probeSideSchema = probeSideSchema;
    this.buildSideSingleBatch = VectorSchemaRoots.slice(allocator, buildSideSingleBatch);
    this.resultConsumer = resultConsumer;
  }

  CrossJoinArrayList(CrossJoinArrayList o) {
    this(
        o.allocator,
        o.outputExpressions,
        o.probeSideSchema,
        o.buildSideSingleBatch,
        o.resultConsumer);
  }

  @Override
  public void close() {
    buildSideSingleBatch.close();
  }

  @Override
  public void flush() throws ComputeException {
  }

  @Override
  public void probe(
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data)
      throws ComputeException {
    try (VectorSchemaRoot flattened =
        VectorSchemaRoots.flatten(allocator, dictionaryProvider, data,null)) {
      probe(flattened);
    }
  }

  private void probe(VectorSchemaRoot probeSideBatch) throws ComputeException {
    Preconditions.checkArgument(probeSideBatch.getSchema().equals(probeSideSchema));

    // TODO: cross join 的扫描侧不需要  probeSideIndices，可以进行优化
    try (ArrayDictionaryProvider outputDictionaryProvider =
             ArrayDictionaryProvider.of(allocator, buildSideSingleBatch, probeSideBatch);
         SelectionBuilder probeSideIndicesBuilder =
             new SelectionBuilder(allocator, "tempProbeSideIndices", probeSideBatch.getRowCount());
         SelectionBuilder buildSideIndicesBuilder =
             new SelectionBuilder(allocator, "tempBuildSideIndices", probeSideBatch.getRowCount());
         MarkBuilder markBuilder = getMarkBuilder(probeSideBatch.getRowCount())) {

      int unmatchedCount =
          outputMatchedAndProbeSideUnmatched(
              buildSideIndicesBuilder,
              probeSideIndicesBuilder,
              markBuilder,
              outputDictionaryProvider,
              probeSideBatch.getRowCount());

      try (IntVector probeSideIndices = probeSideIndicesBuilder.build();
           IntVector buildSideIndices =
               buildSideIndicesBuilder.build(probeSideIndices.getValueCount());
           BitVector mark = markBuilder.build(probeSideIndices.getValueCount());
           BaseIntVector selection = getSelection(probeSideIndices, unmatchedCount)) {
        output(outputDictionaryProvider, buildSideIndices, probeSideIndices, mark, selection);
      }
    }
  }

  protected MarkBuilder getMarkBuilder(int rowCount) {
    return new MarkBuilder();
  }

  protected int outputMatchedAndProbeSideUnmatched(
      SelectionBuilder buildSideIndicesBuilder,
      SelectionBuilder probeSideIndicesBuilder,
      MarkBuilder markBuilder,
      DictionaryProvider dictionary,
      int probeSideBatchRowCount)
      throws ComputeException {
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
      ArrayDictionaryProvider dictionaryProvider,
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

    if(selection!=null){
      try(BaseIntVector selectedBuildSideIndices = ValueVectors.select(allocator, buildSideIndices, selection);
          BaseIntVector selectedProbeSideIndices = probeSideIndices == null
              ? ValueVectors.slice(allocator, selection)
              : ValueVectors.select(allocator, probeSideIndices, selection);
          BitVector selectedMark = ValueVectors.select(allocator, mark, selection)) {
        output(dictionaryProvider, selectedBuildSideIndices, selectedProbeSideIndices, selectedMark, null);
        return;
      }
    }

    int buildSideColumnCount = buildSideSingleBatch.getSchema().getFields().size();
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
            VectorSchemaRoots.create(vectors, buildSideIndices.getValueCount());
        VectorSchemaRoot output =
            ScalarExpressionUtils.evaluate(
                allocator, dictionaryProvider, result, null, outputExpressions)) {
      resultConsumer.consume(dictionaryProvider, output);
    }
  }

  public static class Builder implements JoinCollection.Builder {

    private final BufferAllocator allocator;
    private final Schema buildSideSchema;
    private final Schema probeSideSchema;
    private final List<ScalarExpression<?>> outputExpressions;
    private final List<LazyBatch> buildSideBatches;

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
      outputFields.add(Field.nullable("mark", Types.MinorType.BIT.getType()));
      return ScalarExpressionUtils.getOutputSchema(
          allocator, outputExpressions, new Schema(outputFields));
    }

    @Override
    public void close() {
      buildSideBatches.forEach(LazyBatch::close);
      buildSideBatches.clear();
    }

    @Override
    public void add(
        DictionaryProvider dictionaryProvider,
        VectorSchemaRoot data)
        throws ComputeException {
      buildSideBatches.add(LazyBatch.slice(allocator, dictionaryProvider, data));
    }

    @Override
    public CrossJoinArrayList build(ResultConsumer resultConsumer) throws ComputeException {
      Objects.requireNonNull(resultConsumer);

      try (VectorSchemaRoot buildSideSingleBatch = VectorSchemaRoot.create(buildSideSchema, allocator)) {
        for(LazyBatch buildSideBatch : buildSideBatches) {
          try(VectorSchemaRoot flattened = VectorSchemaRoots.flatten(allocator,buildSideBatch.getDictionaryProvider(),buildSideBatch.getData(),null)){
            VectorSchemaRoots.append(buildSideSingleBatch, flattened);
          }
        }
        return new CrossJoinArrayList(
            allocator, outputExpressions, probeSideSchema, buildSideSingleBatch, resultConsumer);
      }
    }

  }
}
