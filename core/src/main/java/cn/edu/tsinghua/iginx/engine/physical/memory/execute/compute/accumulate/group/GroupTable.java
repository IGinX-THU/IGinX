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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.group;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulators;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseables;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.MaterializedRowKey;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
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
    private final int initialGroupBufferCapacity;
    private final List<ScalarExpression<?>> groupKeyExpressions;
    private final List<ScalarExpression<?>> groupValueExpressions;
    private final List<ExpressionAccumulator> accumulators;
    private final Schema groupKeySchema;
    private final Schema groupValueSchema;
    private final Schema accumulatedSchema;
    private final Schema outputSchema;
    private final HashMap<MaterializedRowKey, GroupState> groups = new HashMap<>();

    public Builder(
        BufferAllocator allocator,
        int maxBatchRowCount,
        int initialGroupBufferCapacity,
        Schema inputSchema,
        List<? extends ScalarExpression<?>> groupKeyExpressions,
        List<? extends ScalarExpression<?>> groupValueExpressions,
        List<ExpressionAccumulator> accumulators)
        throws ComputeException {
      Preconditions.checkArgument(maxBatchRowCount > 0);
      Preconditions.checkArgument(initialGroupBufferCapacity >= 0);
      this.allocator = allocator;
      this.maxBatchRowCount = maxBatchRowCount;
      this.initialGroupBufferCapacity = initialGroupBufferCapacity;
      this.groupKeyExpressions = new ArrayList<>(groupKeyExpressions);
      this.groupValueExpressions = new ArrayList<>(groupValueExpressions);
      this.accumulators = accumulators;
      this.groupKeySchema =
          ScalarExpressionUtils.getOutputSchema(allocator, groupKeyExpressions, inputSchema);
      this.groupValueSchema =
          ScalarExpressionUtils.getOutputSchema(allocator, groupValueExpressions, inputSchema);
      this.accumulatedSchema = ExpressionAccumulators.getOutputSchema(accumulators);
      this.outputSchema =
          PhysicalFunctions.unnest(Schemas.merge(groupKeySchema, accumulatedSchema));
    }

    public void add(VectorSchemaRoot data) throws ComputeException {
      List<GroupState> groupStates = new ArrayList<>(data.getRowCount());
      try (VectorSchemaRoot groupKeys =
          ScalarExpressionUtils.evaluate(allocator, data, groupKeyExpressions)) {
        RowCursor cursor = new RowCursor(groupKeys);
        for (int row = 0; row < groupKeys.getRowCount(); row++) {
          cursor.setPosition(row);
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
          ScalarExpressionUtils.evaluate(allocator, data, groupValueExpressions)) {
        RowCursor cursor = new RowCursor(groupValues);
        for (int row = 0; row < groupStates.size(); row++) {
          cursor.setPosition(row);
          GroupState groupState = groupStates.get(row);
          groupState.update(cursor);
        }
      }
    }

    public GroupTable build() throws ComputeException {
      List<MaterializedRowKey> groupKeys = new ArrayList<>(groups.keySet());
      List<GroupState> groupStates = new ArrayList<>(groups.values());
      List<List<MaterializedRowKey>> groupKeyPartitions =
          Lists.partition(groupKeys, maxBatchRowCount);
      List<List<GroupState>> groupStatePartitions = Lists.partition(groupStates, maxBatchRowCount);

      assert groupKeyPartitions.size() == groupStatePartitions.size();

      try (GroupTable table = new GroupTable(outputSchema)) {
        for (int i = 0; i < groupKeyPartitions.size(); i++) {
          List<MaterializedRowKey> groupKeyPartition = groupKeyPartitions.get(i);
          List<GroupState> groupStatePartition = groupStatePartitions.get(i);
          table.add(build(groupKeyPartition, groupStatePartition));
        }
        return table.transfer();
      }
    }

    private VectorSchemaRoot build(List<MaterializedRowKey> groupKeys, List<GroupState> groupStates)
        throws ComputeException {
      assert groupKeys.size() == groupStates.size();

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
      try (VectorSchemaRoot keysTable =
              MaterializedRowKey.merge(allocator, groupKeySchema, groupKeys);
          VectorSchemaRoot valuesTable =
              ExpressionAccumulators.evaluateSafe(accumulators, statesColumns);
          VectorSchemaRoot joined = VectorSchemaRoots.join(allocator, keysTable, valuesTable)) {
        return PhysicalFunctions.unnest(allocator, joined);
      }
    }

    @Override
    public void close() throws ComputeException {
      ComputingCloseables.close(groups.values());
    }

    public Schema getOutputSchema() {
      return outputSchema;
    }

    private class GroupState implements ComputingCloseable {

      private final List<Accumulator.State> states;
      private final VectorSchemaRoot buffer;
      private final RowCursor target;

      public GroupState() throws ComputeException {
        this.buffer = VectorSchemaRoot.create(groupValueSchema, allocator);
        for (FieldVector vector : buffer.getFieldVectors()) {
          vector.setInitialCapacity(initialGroupBufferCapacity);
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
        target.copyFromSafe(source);
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
