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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.group;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulators;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseables;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ColumnBuilder;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class GroupTable implements AutoCloseable {

  private final Schema schema;
  private final List<VectorSchemaRoot> groups;
  private final List<VectorSchemaRoot> unmodifiableGroups;

  GroupTable(Schema tableSchema) {
    this.schema = tableSchema;
    this.groups = new ArrayList<>();
    this.unmodifiableGroups = Collections.unmodifiableList(groups);
  }

  public Schema getSchema() {
    return schema;
  }

  void add(VectorSchemaRoot data) {
    groups.add(data);
  }

  GroupTable transfer() {
    GroupTable table = new GroupTable(schema);
    table.groups.addAll(groups);
    groups.clear();
    return table;
  }

  public List<VectorSchemaRoot> getGroups() {
    return unmodifiableGroups;
  }

  @Override
  public void close() {
    groups.forEach(VectorSchemaRoot::close);
  }

  public static class Builder implements AutoCloseable {
    private final BufferAllocator allocator;
    private final int maxBatchRowCount;
    private final List<ScalarExpression<?>> groupKeyExpressions;
    private final List<ScalarExpression<?>> groupValueExpressions;
    private final List<ExpressionAccumulator> accumulators;
    private final Schema groupKeySchema;
    private final Schema groupValueSchema;
    private final Schema accumulatedSchema;
    private final HashMap<GroupKey, GroupState> groups = new HashMap<>();

    public Builder(
        BufferAllocator allocator,
        int maxBatchRowCount,
        Schema inputSchema,
        List<? extends ScalarExpression<?>> groupKeyExpressions,
        List<? extends ScalarExpression<?>> groupValueExpressions,
        List<ExpressionAccumulator> accumulators)
        throws ComputeException {
      this.allocator = allocator;
      this.maxBatchRowCount = maxBatchRowCount;
      this.groupKeyExpressions = new ArrayList<>(groupKeyExpressions);
      this.groupValueExpressions = new ArrayList<>(groupValueExpressions);
      this.accumulators = accumulators;
      this.groupKeySchema =
          ScalarExpressions.getOutputSchema(allocator, groupKeyExpressions, inputSchema);
      this.groupValueSchema =
          ScalarExpressions.getOutputSchema(allocator, groupValueExpressions, inputSchema);
      this.accumulatedSchema = ExpressionAccumulators.getOutputSchema(accumulators);
    }

    public void add(VectorSchemaRoot data) throws ComputeException {
      List<GroupState> groupStates = new ArrayList<>(data.getRowCount());
      try (VectorSchemaRoot groupKeys =
          ScalarExpressions.evaluateSafe(allocator, groupKeyExpressions, data)) {
        for (int row = 0; row < data.getRowCount(); row++) {
          GroupKey groupKey = new GroupKey(groupKeys, row);
          GroupState groupState = groups.get(groupKey);
          if (groupState == null) {
            groupState = new GroupState();
            groups.put(groupKey, groupState);
          }
          groupStates.add(groupState);
        }
      }
      try (VectorSchemaRoot groupValues =
          ScalarExpressions.evaluateSafe(allocator, groupValueExpressions, data)) {
        FieldVector[] groupValuesVectors =
            groupValues.getFieldVectors().toArray(new FieldVector[0]);
        for (int row = 0; row < groupStates.size(); row++) {
          GroupState groupState = groupStates.get(row);
          groupState.update(groupValuesVectors, row);
        }
      }
    }

    public GroupTable build() throws ComputeException {
      Schema tableSchema = Schemas.merge(groupKeySchema, accumulatedSchema);
      try (GroupTable table = new GroupTable(tableSchema)) {
        List<Map.Entry<GroupKey, GroupState>> entryBatch = new ArrayList<>(maxBatchRowCount);
        for (Map.Entry<GroupKey, GroupState> entry : groups.entrySet()) {
          entryBatch.add(entry);
          if (entryBatch.size() >= maxBatchRowCount) {
            table.add(build(entryBatch));
            entryBatch.clear();
          }
        }
        if (!entryBatch.isEmpty()) {
          table.add(build(entryBatch));
        }
        return table.transfer();
      }
    }

    private VectorSchemaRoot build(List<Map.Entry<GroupKey, GroupState>> entries)
        throws ComputeException {
      List<Object[]> groupKeys =
          entries.stream()
              .map(Map.Entry::getKey)
              .map(GroupKey::getData)
              .collect(Collectors.toList());
      List<GroupState> groupStates =
          entries.stream().map(Map.Entry::getValue).collect(Collectors.toList());
      List<List<Accumulator.State>> statesColumns = new ArrayList<>(accumulators.size());
      for (int i = 0; i < accumulators.size(); i++) {
        statesColumns.add(new ArrayList<>());
      }
      for (GroupState groupState : groupStates) {
        groupState.flush();
        for (int i = 0; i < accumulators.size(); i++) {
          statesColumns.get(i).add(groupState.states.get(i));
        }
      }
      try (VectorSchemaRoot keysTable = GroupKey.merge(allocator, groupKeySchema, groupKeys);
          VectorSchemaRoot valuesTable =
              ExpressionAccumulators.evaluateSafe(accumulators, statesColumns)) {
        return VectorSchemaRoots.join(allocator, keysTable, valuesTable);
      }
    }

    @Override
    public void close() throws ComputeException {
      ComputingCloseables.close(groups.values());
    }

    private class GroupState implements ComputingCloseable {

      private final List<Accumulator.State> states;
      private final VectorSchemaRoot buffer;
      private final FieldVector[] vectors;
      private int rowCount = 0;

      public GroupState() throws ComputeException {
        this.buffer = VectorSchemaRoot.create(groupValueSchema, allocator);
        for (FieldVector vector : buffer.getFieldVectors()) {
          vector.setInitialCapacity(maxBatchRowCount);
          if (vector instanceof FixedWidthVector) {
            ((FixedWidthVector) vector).allocateNew(maxBatchRowCount);
          }
        }
        this.vectors = buffer.getFieldVectors().toArray(new FieldVector[0]);
        this.states = new ArrayList<>(accumulators.size());
        try {
          for (Accumulator accumulator : accumulators) {
            states.add(accumulator.createState());
          }
        } catch (ComputeException e) {
          close();
          throw e;
        }
      }

      public void update(FieldVector[] sources, int index) throws ComputeException {
        for (int i = 0; i < sources.length; i++) {
          FieldVector source = sources[i];
          FieldVector target = vectors[i];
          if (source instanceof FixedWidthVector) {
            target.copyFrom(index, rowCount, source);
          } else {
            target.copyFromSafe(index, rowCount, source);
          }
        }
        rowCount++;
        if (rowCount >= maxBatchRowCount) {
          flush();
        }
      }

      public void flush() throws ComputeException {
        if (rowCount > 0) {
          buffer.setRowCount(rowCount);
          ExpressionAccumulators.update(accumulators, states, buffer);
          rowCount = 0;
        }
      }

      @Override
      public void close() throws ComputeException {
        buffer.close();
        ComputingCloseables.close(states);
      }
    }
  }

  private static final class GroupKey {
    private final Object[] key;
    private final transient int hashCode;

    public GroupKey(VectorSchemaRoot data, int index) {
      this(VectorSchemaRoots.get(data, index));
    }

    GroupKey(Object[] key) {
      this.key = key;
      hashCode = Arrays.deepHashCode(key);
    }

    public static VectorSchemaRoot merge(
        BufferAllocator allocator, Schema groupKeySchema, List<Object[]> groupKeys)
        throws ComputeException {
      VectorSchemaRoot root = VectorSchemaRoot.create(groupKeySchema, allocator);
      List<ColumnBuilder> columnBuilders = new ArrayList<>();
      try {
        for (FieldVector fieldVector : root.getFieldVectors()) {
          if (!ColumnBuilder.support(fieldVector)) {
            throw new ComputeException("Unsupported type: " + fieldVector.getMinorType());
          }
          columnBuilders.add(ColumnBuilder.create(fieldVector));
          fieldVector.setInitialCapacity(groupKeys.size());
        }
      } catch (ComputeException e) {
        root.close();
        throw e;
      }
      for (Object[] groupKey : groupKeys) {
        for (int i = 0; i < groupKey.length; i++) {
          columnBuilders.get(i).append(groupKey[i]);
        }
      }
      root.setRowCount(groupKeys.size());
      return root;
    }

    public Object[] getData() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey groupKey = (GroupKey) o;
      return Arrays.deepEquals(key, groupKey.key);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
