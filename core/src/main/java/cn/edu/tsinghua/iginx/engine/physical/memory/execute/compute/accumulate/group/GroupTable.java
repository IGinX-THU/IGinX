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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.MaterializedRowKey;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;
import java.util.stream.Collectors;

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
    private final HashMap<MaterializedRowKey, GroupState> groups = new HashMap<>();

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
        RowCursor cursor = new RowCursor(groupKeys);
        for (int row = 0; row < data.getRowCount(); row++) {
          MaterializedRowKey materializedRowKey = MaterializedRowKey.of(cursor);
          GroupState groupState = groups.get(materializedRowKey);
          if (groupState == null) {
            groupState = new GroupState();
            groups.put(materializedRowKey, groupState);
          }
          groupStates.add(groupState);
        }
      }
      try (VectorSchemaRoot groupValues =
               ScalarExpressions.evaluateSafe(allocator, groupValueExpressions, data)) {
        RowCursor cursor = new RowCursor(data);
        for (int row = 0; row < groupStates.size(); row++) {
          cursor.setPosition(row);
          GroupState groupState = groupStates.get(row);
          groupState.update(cursor);
        }
      }
    }

    public GroupTable build() throws ComputeException {
      Schema tableSchema = Schemas.merge(groupKeySchema, accumulatedSchema);
      try (GroupTable table = new GroupTable(tableSchema)) {
        List<Map.Entry<MaterializedRowKey, GroupState>> entryBatch = new ArrayList<>(maxBatchRowCount);
        for (Map.Entry<MaterializedRowKey, GroupState> entry : groups.entrySet()) {
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

    private VectorSchemaRoot build(List<Map.Entry<MaterializedRowKey, GroupState>> entries)
        throws ComputeException {
      List<MaterializedRowKey> groupKeys =
          entries.stream().map(Map.Entry::getKey).collect(Collectors.toList());
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
      try (VectorSchemaRoot keysTable = MaterializedRowKey.merge(allocator, groupKeySchema, groupKeys);
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
      private final RowCursor target;

      public GroupState() throws ComputeException {
        this.buffer = VectorSchemaRoot.create(groupValueSchema, allocator);
        for (FieldVector vector : buffer.getFieldVectors()) {
          vector.setInitialCapacity(maxBatchRowCount);
          if (vector instanceof FixedWidthVector) {
            ((FixedWidthVector) vector).allocateNew(maxBatchRowCount);
          }
        }
        this.target = new RowCursor(buffer);
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

      public void update(RowCursor source) throws ComputeException {
        target.copyFrom(source);
        target.setPosition(target.getPosition() + 1);
        if (target.getPosition() >= maxBatchRowCount) {
          flush();
        }
      }

      public void flush() throws ComputeException {
        if (target.getPosition() > 0) {
          buffer.setRowCount(target.getPosition());
          ExpressionAccumulators.update(accumulators, states, buffer);
          target.setPosition(0);
        }
      }

      @Override
      public void close() throws ComputeException {
        buffer.close();
        ComputingCloseables.close(states);
      }
    }
  }

}
