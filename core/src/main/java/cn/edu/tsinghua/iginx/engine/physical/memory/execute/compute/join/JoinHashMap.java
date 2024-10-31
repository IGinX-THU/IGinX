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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.compare.Equal;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash.Hash;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import com.google.common.collect.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinHashMap implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int batchSize;

  private final List<VectorSchemaRoot> stagedKeysList;
  private final List<VectorSchemaRoot> stagedValuesList;
  private final Hash hasher;
  private final ListMultimap<Integer, JoinCursor> map;

  private final List<ScalarExpression<?>> probeSideKeyExpressions;
  private final List<ScalarExpression<?>> probeSideValueExpressions;
  private final Schema probeSideSchema;
  private final Schema equalityKeysSchema;

  private final JoinOutputBuffer buffer;

  private final ScalarExpression<BitVector> equalityKeysPairTestExpressions;

  protected JoinHashMap(
      BufferAllocator allocator,
      int batchSize,
      List<VectorSchemaRoot> stagedKeysList,
      List<VectorSchemaRoot> stagedValuesList,
      Hash hasher,
      ListMultimap<Integer, JoinCursor> map,
      List<ScalarExpression<?>> probeSideKeyExpressions,
      List<ScalarExpression<?>> probeSideValueExpressions,
      Schema probeSideSchema,
      Schema buildSideValuesSchema)
      throws ComputeException {
    this.allocator = allocator;
    this.batchSize = batchSize;

    this.stagedKeysList = stagedKeysList;
    this.stagedValuesList = stagedValuesList;
    this.hasher = hasher;
    this.map = map;

    this.probeSideKeyExpressions = probeSideKeyExpressions;
    this.probeSideValueExpressions = probeSideValueExpressions;
    this.probeSideSchema = probeSideSchema;
    this.equalityKeysSchema =
        ScalarExpressions.getOutputSchema(allocator, probeSideKeyExpressions, probeSideSchema);
    Schema probeSideValuesSchema =
        ScalarExpressions.getOutputSchema(allocator, probeSideValueExpressions, probeSideSchema);

    this.buffer =
        new JoinOutputBuffer(allocator, batchSize, buildSideValuesSchema, probeSideValuesSchema);

    int equalityFieldCount = probeSideKeyExpressions.size();
    this.equalityKeysPairTestExpressions =
        IntStream.range(0, equalityFieldCount)
            .mapToObj(
                i ->
                    new CallNode<>(
                        new Equal(), new FieldNode(i), new FieldNode(i + equalityFieldCount)))
            .reduce((l, r) -> new CallNode<>(new And(), l, r))
            .orElseThrow(() -> new IllegalStateException("No equality expressions"));
  }

  @Override
  public void close() {
    stagedKeysList.forEach(VectorSchemaRoot::close);
    stagedValuesList.forEach(VectorSchemaRoot::close);
    buffer.close();
  }

  public JoinOutputBuffer getOutput() {
    return buffer;
  }

  public void probe(VectorSchemaRoot batch, boolean needProbeSideUnmatched)
      throws ComputeException {
    Preconditions.checkArgument(
        Objects.equals(batch.getSchema(), probeSideSchema),
        "Batch schema does not match probe side schema");

    try (VectorSchemaRoot keys =
            ScalarExpressions.evaluateSafe(allocator, probeSideKeyExpressions, batch);
        VectorSchemaRoot values =
            ScalarExpressions.evaluateSafe(allocator, probeSideValueExpressions, batch)) {
      List<JoinCursor> buildSideCandidates = new ArrayList<>();
      List<JoinCursor> probeSideCandidates = new ArrayList<>();

      try (IntVector hashCodes = hasher.invoke(allocator, keys)) {
        JoinCursor cursorTemplate = new JoinCursor(keys, values);
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
      if (needProbeSideUnmatched) {
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

    try (VectorSchemaRoot equalityKeysPair =
            buildEqualityKeysPair(buildSideCandidates, probeSideCandidates);
        BitVector mask = equalityKeysPairTestExpressions.invoke(allocator, equalityKeysPair)) {
      for (int i = 0; i < buildSideCandidates.size(); i++) {
        if (!mask.isNull(i) && mask.get(i) != 0) {
          buildSideMatched.add(buildSideCandidates.get(i));
          probeSideMatched.add(probeSideCandidates.get(i));
        }
      }
    }

    buffer.append(buildSideMatched, probeSideMatched);
  }

  private VectorSchemaRoot buildEqualityKeysPair(
      List<JoinCursor> buildSideCandidates, List<JoinCursor> probeSideCandidates) {
    assert buildSideCandidates.size() == probeSideCandidates.size();
    try (VectorSchemaRoot probeSideKeys = buildEqualityKeys(probeSideCandidates);
        VectorSchemaRoot buildSideKeys = buildEqualityKeys(buildSideCandidates)) {
      return VectorSchemaRoots.join(allocator, buildSideKeys, probeSideKeys);
    }
  }

  private VectorSchemaRoot buildEqualityKeys(List<JoinCursor> candidates) {
    VectorSchemaRoot buildSideKeys = VectorSchemaRoot.create(equalityKeysSchema, allocator);
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
    private final List<ScalarExpression<?>> buildSideKeyExpressions;
    private final List<ScalarExpression<?>> buildSideValueExpressions;
    private final List<VectorSchemaRoot> stagedKeysList;
    private final List<VectorSchemaRoot> stagedValuesList;
    private final Hash hasher = new Hash();
    private final ListMultimap<Integer, JoinCursor> map;
    private boolean closed = false;

    public Builder(
        BufferAllocator allocator,
        Schema buildSideSchema,
        List<ScalarExpression<?>> buildSideKeyExpressions,
        List<ScalarExpression<?>> buildSideValueExpressions) {
      Preconditions.checkArgument(!buildSideKeyExpressions.isEmpty());

      this.allocator = Objects.requireNonNull(allocator);
      this.map = MultimapBuilder.hashKeys().arrayListValues().build();
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.buildSideKeyExpressions = new ArrayList<>(buildSideKeyExpressions);
      this.buildSideValueExpressions = new ArrayList<>(buildSideValueExpressions);
      this.stagedKeysList = new ArrayList<>();
      this.stagedValuesList = new ArrayList<>();
    }

    public Builder add(VectorSchemaRoot batch) throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkArgument(
          Objects.equals(batch.getSchema(), buildSideSchema),
          "Batch schema does not match build side schema");

      VectorSchemaRoot keys =
          ScalarExpressions.evaluateSafe(allocator, buildSideKeyExpressions, batch);
      stagedKeysList.add(keys);

      VectorSchemaRoot values =
          ScalarExpressions.evaluateSafe(allocator, buildSideValueExpressions, batch);
      stagedValuesList.add(values);

      JoinCursor cursorTemplate = new JoinCursor(keys, values);
      try (IntVector hashCodes = hasher.invoke(allocator, keys)) {
        for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
          map.put(hashCodes.get(rowIndex), cursorTemplate.withPosition(rowIndex));
        }
      }

      return this;
    }

    public JoinHashMap build(
        int batchSize,
        Schema probeSideSchema,
        List<ScalarExpression<?>> probeSideKeyExpressions,
        List<ScalarExpression<?>> probeSideValueExpressions)
        throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkNotNull(allocator, "Allocator is required");
      Preconditions.checkNotNull(probeSideSchema, "Probe side schema is required");
      Preconditions.checkNotNull(
          probeSideKeyExpressions, "Build side key expressions are required");
      Preconditions.checkNotNull(
          probeSideValueExpressions, "Build side value expressions are required");
      closed = true;

      Schema probeKeySchema =
          ScalarExpressions.getOutputSchema(allocator, probeSideKeyExpressions, probeSideSchema);
      Schema buildSideKeySchema =
          ScalarExpressions.getOutputSchema(allocator, buildSideKeyExpressions, buildSideSchema);

      Preconditions.checkArgument(probeKeySchema.getFields().size() == buildSideKeySchema.getFields().size());
      Streams.zip(
          probeKeySchema.getFields().stream(),
          buildSideKeySchema.getFields().stream(),
          (l, r) -> l.getType().equals(r.getType()))
          .forEach(Preconditions::checkArgument);

      Schema buildSideValuesSchema =
          ScalarExpressions.getOutputSchema(allocator, buildSideValueExpressions, buildSideSchema);
      return new JoinHashMap(
          allocator,
          batchSize,
          stagedKeysList,
          stagedValuesList,
          hasher,
          map,
          probeSideKeyExpressions,
          probeSideValueExpressions,
          probeSideSchema,
          buildSideValuesSchema);
    }

    @Override
    public void close(){
      if (!closed) {
        stagedKeysList.forEach(VectorSchemaRoot::close);
        stagedValuesList.forEach(VectorSchemaRoot::close);
        closed = true;
      }
    }
  }
}
