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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class AddSequenceExecutor extends StatefulUnaryExecutor {

  private final long[] starts;
  private final long[] increments;
  private final String[] columns;

  public static AddSequenceExecutor of(
      ExecutorContext context,
      Schema inputSchema,
      List<Long> starts,
      List<Long> increments,
      List<String> columns) {
    long[] startsArray = starts.stream().mapToLong(Long::longValue).toArray();
    long[] incrementsArray = increments.stream().mapToLong(Long::longValue).toArray();
    String[] columnsArray = columns.stream().map(Objects::requireNonNull).toArray(String[]::new);
    return new AddSequenceExecutor(
        context, inputSchema, startsArray, incrementsArray, columnsArray);
  }

  protected AddSequenceExecutor(
      ExecutorContext context,
      Schema inputSchema,
      long[] starts,
      long[] increments,
      String[] columns) {
    super(context, inputSchema, 1);
    this.starts = Objects.requireNonNull(starts);
    this.increments = Objects.requireNonNull(increments);
    this.columns = Objects.requireNonNull(columns);
  }

  private long offset = 0;

  @Override
  protected void consumeUnchecked(Batch batch) {
    VectorSchemaRoot flattened = batch.flattened(context.getAllocator());
    List<FieldVector> vectors = new ArrayList<>(flattened.getFieldVectors());
    for (int i = 0; i < columns.length; i++) {
      long increment = increments[i];
      long start = starts[i] + offset * increment;
      String name = columns[i];
      BigIntVector sequence =
          createSequenceVector(
              context.getAllocator(), name, start, increment, flattened.getRowCount());
      vectors.add(sequence);
    }
    offset += flattened.getRowCount();
    try (Batch output = Batch.of(VectorSchemaRoots.create(vectors, flattened.getRowCount()))) {
      offerResult(output);
    }
  }

  private static BigIntVector createSequenceVector(
      BufferAllocator allocator, String name, long start, long increment, int size) {
    BigIntVector vector =
        (BigIntVector)
            Field.notNullable(name, Types.MinorType.BIGINT.getType()).createVector(allocator);
    vector.allocateNew(size);
    vector.setValueCount(size);
    for (int index = 0; index < size; index++) {
      vector.set(index, start + index * increment);
    }
    return vector;
  }

  @Override
  protected void consumeEndUnchecked() throws ComputeException {}

  private Schema outputSchema = null;

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      List<Field> fields = new ArrayList<>(inputSchema.getFields());
      for (String column : columns) {
        fields.add(Field.notNullable(column, Types.MinorType.BIGINT.getType()));
      }
      outputSchema = new Schema(fields);
    }
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    StringJoiner joiner = new StringJoiner(", ", "AddSequence[", "]");
    for (int i = 0; i < starts.length; i++) {
      joiner.add(columns[i] + "(" + starts[i] + ", " + increments[i] + ")");
    }
    return joiner.toString();
  }
}
