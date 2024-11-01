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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MultimapBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinHashMap implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int batchSize;
  private final JoinOption joinOption;

  private final ScalarExpression<IntVector> hasher;
  private final ScalarExpression<BitVector> tester;
  private final ListMultimap<Integer, JoinCursor> map;

  private final Schema buildSideOnFieldsSchema;
  private final List<VectorSchemaRoot> buildSideOnFieldsList;
  private final List<VectorSchemaRoot> buildSideOutputFieldsList;

  private final Schema probeSideSchema;
  private final Schema probeSideOnFieldsSchema;
  private final List<ScalarExpression<?>> probeSideOnFieldExpressions;
  private final List<ScalarExpression<?>> probeSideOutputFieldExpressions;

  private final JoinOutputBuffer buffer;

  JoinHashMap(
      BufferAllocator allocator,
      int batchSize,
      JoinOption joinOption,
      ScalarExpression<BitVector> tester,
      ScalarExpression<IntVector> hasher,
      ListMultimap<Integer, JoinCursor> map,
      Schema buildSideOnSchema,
      Schema buildSideOutputSchema,
      List<VectorSchemaRoot> buildSideOnFieldsList,
      List<VectorSchemaRoot> buildSideOutputFieldsList,
      Schema probeSideSchema,
      List<ScalarExpression<?>> probeSideOnFieldExpressions,
      List<ScalarExpression<?>> probeSideOutputFieldExpressions)
      throws ComputeException {
    this.allocator = allocator;
    this.batchSize = batchSize;
    this.joinOption = joinOption;
    this.hasher = hasher;
    this.tester = tester;
    this.map = map;
    this.buildSideOnFieldsSchema = buildSideOnSchema;
    this.buildSideOnFieldsList = buildSideOnFieldsList;
    this.buildSideOutputFieldsList = buildSideOutputFieldsList;
    this.probeSideSchema = probeSideSchema;
    this.probeSideOnFieldsSchema =
        ScalarExpressions.getOutputSchema(allocator, probeSideOnFieldExpressions, probeSideSchema);
    Schema probeSideOutputSchema =
        ScalarExpressions.getOutputSchema(
            allocator, probeSideOutputFieldExpressions, probeSideSchema);
    this.probeSideOnFieldExpressions = probeSideOnFieldExpressions;
    this.probeSideOutputFieldExpressions = probeSideOutputFieldExpressions;
    this.buffer =
        new JoinOutputBuffer(
            allocator, batchSize, joinOption, buildSideOutputSchema, probeSideOutputSchema);
  }

  @Override
  public void close() {
    buildSideOnFieldsList.forEach(VectorSchemaRoot::close);
    buildSideOutputFieldsList.forEach(VectorSchemaRoot::close);
    buffer.close();
  }

  public JoinOutputBuffer getOutput() {
    return buffer;
  }

  public void probe(VectorSchemaRoot batch) throws ComputeException {
    Preconditions.checkArgument(
        Objects.equals(batch.getSchema(), probeSideSchema),
        "Batch schema does not match probe side schema");

    try (VectorSchemaRoot onFields =
            ScalarExpressions.evaluateSafe(allocator, probeSideOnFieldExpressions, batch);
        VectorSchemaRoot outputFields =
            ScalarExpressions.evaluateSafe(allocator, probeSideOutputFieldExpressions, batch)) {
      List<JoinCursor> buildSideCandidates = new ArrayList<>();
      List<JoinCursor> probeSideCandidates = new ArrayList<>();

      try (IntVector hashCodes = ScalarExpressions.evaluateSafe(allocator, hasher, batch)) {
        JoinCursor cursorTemplate = new JoinCursor(onFields, outputFields);
        for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
          int hashCode = hashCodes.get(rowIndex);
          JoinCursor probeSideCandidate = cursorTemplate.withPosition(rowIndex);
          for (JoinCursor buildSideCandidate : map.get(hashCode)) {
            buildSideCandidates.add(buildSideCandidate);
            probeSideCandidates.add(probeSideCandidate);
          }
        }
      }

      outputAllMatched(buildSideCandidates, probeSideCandidates);
      if (joinOption.needOutputProbeSideUnmatched()) {
        buffer.append(
            Collections.emptyList(),
            Iterables.filter(probeSideCandidates, JoinCursor::isUnmatched));
      }
    }
  }

  public void outputBuildSideUnmatched() {
    buffer.append(Iterables.filter(map.values(), JoinCursor::isUnmatched), Collections.emptyList());
  }

  private void outputAllMatched(
      List<JoinCursor> buildSideCandidates, List<JoinCursor> probeSideCandidates)
      throws ComputeException {
    assert buildSideCandidates.size() == probeSideCandidates.size();

    List<List<JoinCursor>> buildSidePartitionedCandidates =
        Lists.partition(buildSideCandidates, batchSize);
    List<List<JoinCursor>> probeSidePartitionedCandidates =
        Lists.partition(probeSideCandidates, batchSize);

    assert buildSidePartitionedCandidates.size() == probeSidePartitionedCandidates.size();

    for (int i = 0; i < buildSidePartitionedCandidates.size(); i++) {
      List<JoinCursor> buildSideCandidatesPartition = buildSidePartitionedCandidates.get(i);
      List<JoinCursor> probeSideCandidatesPartition = probeSidePartitionedCandidates.get(i);
      outputPartitionMatched(buildSideCandidatesPartition, probeSideCandidatesPartition);
    }
  }

  private void outputPartitionMatched(
      List<JoinCursor> buildSideCandidates, List<JoinCursor> probeSideCandidates)
      throws ComputeException {
    assert buildSideCandidates.size() == probeSideCandidates.size();

    List<JoinCursor> buildSideMatched = new ArrayList<>();
    List<JoinCursor> probeSideMatched = new ArrayList<>();

    try (VectorSchemaRoot onFieldsPair =
            buildOnFieldsPair(buildSideCandidates, probeSideCandidates);
        BitVector mask = tester.invoke(allocator, onFieldsPair)) {
      for (int i = 0; i < buildSideCandidates.size(); i++) {
        if (!mask.isNull(i) && mask.get(i) != 0) {
          JoinCursor buildSideCursor = buildSideCandidates.get(i);
          JoinCursor probeSideCursor = probeSideCandidates.get(i);
          if (probeSideCursor.isMatched()) {
            if (!joinOption.allowProbeSideMatchMultiple()) {
              throw new ComputeException("the return value of sub-query has more than one rows");
            }
            if (!joinOption.needAllMatched()) {
              continue;
            }
          }
          buildSideCursor.markMatched();
          probeSideCursor.markMatched();
          buildSideMatched.add(buildSideCursor);
          probeSideMatched.add(probeSideCursor);
        }
      }
    }

    buffer.append(buildSideMatched, probeSideMatched);
  }

  private VectorSchemaRoot buildOnFieldsPair(
      List<JoinCursor> buildSideCandidates, List<JoinCursor> probeSideCandidates) {
    assert buildSideCandidates.size() == probeSideCandidates.size();
    try (VectorSchemaRoot probeSideOnFields =
            buildOnFields(probeSideOnFieldsSchema, probeSideCandidates);
        VectorSchemaRoot buildSideOnFields =
            buildOnFields(buildSideOnFieldsSchema, buildSideCandidates)) {
      return VectorSchemaRoots.join(allocator, buildSideOnFields, probeSideOnFields);
    }
  }

  private VectorSchemaRoot buildOnFields(Schema schema, List<JoinCursor> candidates) {
    VectorSchemaRoot buildSideKeys = VectorSchemaRoot.create(schema, allocator);
    buildSideKeys.getFieldVectors().forEach(v -> v.setInitialCapacity(candidates.size()));
    buildSideKeys.setRowCount(candidates.size());
    RowCursor cursor = new RowCursor(buildSideKeys);
    for (int i = 0; i < candidates.size(); i++) {
      cursor.setPosition(i);
      candidates.get(i).copyKeysTo(cursor);
    }
    return buildSideKeys;
  }

  public static class Builder implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Schema buildSideSchema;
    private final JoinOption joinOption;
    private final ScalarExpression<IntVector> hasher;
    private final List<ScalarExpression<?>> buildSideOnFieldExpressions;
    private final List<ScalarExpression<?>> buildSideOutputValueExpressions;
    private final List<VectorSchemaRoot> stagedOnFieldsList;
    private final List<VectorSchemaRoot> stagedOutputValuesList;
    private final ListMultimap<Integer, JoinCursor> map;
    private boolean closed = false;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        JoinOption joinOption,
        ScalarExpression<IntVector> hasher,
        List<ScalarExpression<?>> buildSideOnFieldExpressions,
        List<ScalarExpression<?>> buildSideOutputValueExpressions) {
      Preconditions.checkArgument(!buildSideOnFieldExpressions.isEmpty());

      this.allocator = Objects.requireNonNull(allocator);
      this.joinOption = Objects.requireNonNull(joinOption);
      this.map = MultimapBuilder.hashKeys().arrayListValues().build();
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.hasher = Objects.requireNonNull(hasher);
      this.buildSideOnFieldExpressions = new ArrayList<>(buildSideOnFieldExpressions);
      this.buildSideOutputValueExpressions = new ArrayList<>(buildSideOutputValueExpressions);
      this.stagedOnFieldsList = new ArrayList<>();
      this.stagedOutputValuesList = new ArrayList<>();
    }

    public Builder add(VectorSchemaRoot batch) throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkArgument(
          Objects.equals(batch.getSchema(), buildSideSchema),
          "Batch schema does not match build side schema");

      VectorSchemaRoot onFields =
          ScalarExpressions.evaluateSafe(allocator, buildSideOnFieldExpressions, batch);
      stagedOnFieldsList.add(onFields);

      VectorSchemaRoot outputValues =
          ScalarExpressions.evaluateSafe(allocator, buildSideOutputValueExpressions, batch);
      stagedOutputValuesList.add(outputValues);

      JoinCursor cursorTemplate = new JoinCursor(onFields, outputValues);
      try (IntVector hashCodes = ScalarExpressions.evaluateSafe(allocator, hasher, batch)) {
        for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
          map.put(hashCodes.get(rowIndex), cursorTemplate.withPosition(rowIndex));
        }
      }

      return this;
    }

    public JoinHashMap build(
        int batchSize,
        Schema probeSideSchema,
        ScalarExpression<IntVector> probeSideHasher,
        List<ScalarExpression<?>> probeSideOnFieldExpressions,
        List<ScalarExpression<?>> probeSideOutputFieldExpressions,
        ScalarExpression<BitVector> OnFieldsPairTester)
        throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkNotNull(allocator, "Allocator is required");
      Preconditions.checkNotNull(probeSideSchema, "Probe side schema is required");
      Preconditions.checkNotNull(
          probeSideOnFieldExpressions, "Build side key expressions are required");
      Preconditions.checkNotNull(
          probeSideOutputFieldExpressions, "Build side value expressions are required");
      closed = true;

      Schema buildSideOnFieldsSchema =
          ScalarExpressions.getOutputSchema(
              allocator, buildSideOnFieldExpressions, buildSideSchema);
      Schema buildSideOutputSchema =
          ScalarExpressions.getOutputSchema(
              allocator, buildSideOutputValueExpressions, buildSideSchema);

      return new JoinHashMap(
          allocator,
          batchSize,
          joinOption,
          OnFieldsPairTester,
          probeSideHasher,
          map,
          buildSideOnFieldsSchema,
          buildSideOutputSchema,
          stagedOnFieldsList,
          stagedOutputValuesList,
          probeSideSchema,
          probeSideOnFieldExpressions,
          probeSideOutputFieldExpressions);
    }

    @Override
    public void close() {
      if (!closed) {
        stagedOnFieldsList.forEach(VectorSchemaRoot::close);
        stagedOutputValuesList.forEach(VectorSchemaRoot::close);
        closed = true;
      }
    }
  }
}
