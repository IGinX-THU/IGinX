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
import java.util.List;
import java.util.Objects;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinArrayList implements AutoCloseable {

  private final BufferAllocator allocator;
  private final List<ScalarExpression<?>> outputExpressions;
  private final Schema probeSideSchema;
  private final VectorSchemaRoot buildSideSingleBatch;
  private final ResultConsumer resultConsumer;

  JoinArrayList(
      BufferAllocator allocator,
      List<ScalarExpression<?>> outputExpressions,
      Schema probeSideSchema,
      @WillCloseWhenClosed VectorSchemaRoot buildSideSingleBatch,
      ResultConsumer resultConsumer) {
    this.allocator = allocator;
    this.outputExpressions = outputExpressions;
    this.probeSideSchema = probeSideSchema;
    this.buildSideSingleBatch = buildSideSingleBatch;
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

      for (int probeSideIndex = 0;
          probeSideIndex < probeSideBatch.getRowCount();
          probeSideIndex++) {
        for (int buildSideIndex = 0;
            buildSideIndex < buildSideSingleBatch.getRowCount();
            buildSideIndex++) {
          buildSideCandidateIndicesBuilder.append(buildSideIndex);
          probeSideCandidateIndicesBuilder.append(probeSideIndex);
        }
      }

      try (ArrayDictionaryProvider outputDictionaryProvider =
              ArrayDictionaryProvider.of(allocator, buildSideSingleBatch, probeSideBatch);
          IntVector buildSideIndices = buildSideCandidateIndicesBuilder.build();
          IntVector probeSideIndices = probeSideCandidateIndicesBuilder.build()) {
        output(outputDictionaryProvider, buildSideIndices, probeSideIndices);
      }
    }
  }

  private void output(
      ArrayDictionaryProvider dictionaryProvider,
      BaseIntVector buildSideIndices,
      BaseIntVector probeSideIndices)
      throws ComputeException {
    Preconditions.checkArgument(
        buildSideIndices.getValueCount() == probeSideIndices.getValueCount());

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

    try (VectorSchemaRoot result =
            VectorSchemaRoots.create(vectors, probeSideIndices.getValueCount());
        VectorSchemaRoot output =
            ScalarExpressionUtils.evaluate(
                allocator, dictionaryProvider, result, null, outputExpressions)) {
      resultConsumer.consume(
          dictionaryProvider.slice(allocator), VectorSchemaRoots.transfer(allocator, output), null);
    }
  }

  public static class Builder implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Schema buildSideSchema;
    private final List<VectorSchemaRoot> buildSideBatches;

    public Builder(BufferAllocator allocator, Schema buildSideSchema) {
      this.allocator = Objects.requireNonNull(allocator);
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.buildSideBatches = new ArrayList<>();
    }

    @Override
    public void close() {
      buildSideBatches.forEach(VectorSchemaRoot::close);
      buildSideBatches.clear();
    }

    public Builder add(@WillClose VectorSchemaRoot buildSideBatch) throws ComputeException {
      buildSideBatches.add(buildSideBatch);
      return this;
    }

    public JoinArrayList build(
        BufferAllocator allocator,
        List<ScalarExpression<?>> outputExpressions,
        Schema probeSideSchema,
        ResultConsumer resultConsumer)
        throws ComputeException {
      Preconditions.checkNotNull(allocator);
      Preconditions.checkNotNull(probeSideSchema);
      Preconditions.checkNotNull(resultConsumer);

      try (VectorSchemaRoot buildSideSingleBatch =
          VectorSchemaRoots.concat(allocator, buildSideSchema, buildSideBatches)) {
        return new JoinArrayList(
            allocator,
            outputExpressions,
            probeSideSchema,
            VectorSchemaRoots.transfer(allocator, buildSideSingleBatch),
            resultConsumer);
      }
    }
  }
}
