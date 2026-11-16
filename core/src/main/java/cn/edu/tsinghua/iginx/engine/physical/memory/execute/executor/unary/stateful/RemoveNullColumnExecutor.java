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
import java.util.*;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class RemoveNullColumnExecutor extends StatefulUnaryExecutor {

  private final Queue<VectorSchemaRoot> data = new ArrayDeque<>();
  private Schema outputSchema;

  public RemoveNullColumnExecutor(ExecutorContext context, Schema inputSchema) {
    super(context, inputSchema, 1);
  }

  public void close() throws ComputeException {
    data.forEach(VectorSchemaRoot::close);
    data.clear();
    super.close();
  }

  @Override
  protected void consumeUnchecked(Batch batch) {
    data.add(batch.flattened(context.getAllocator()));
  }

  @Override
  protected void consumeEndUnchecked() {
    if (data.stream().allMatch(group -> group.getRowCount() == 0)) {
      outputSchema = inputSchema;
      return;
    }
    boolean[] isNullColumn = new boolean[inputSchema.getFields().size()];
    Arrays.fill(isNullColumn, true);
    for (VectorSchemaRoot vectorSchemaRoot : data) {
      for (int i = 0; i < isNullColumn.length; i++) {
        FieldVector vector = vectorSchemaRoot.getVector(i);
        boolean allNull =
            vector.getField().isNullable() && vector.getNullCount() == vector.getValueCount();
        isNullColumn[i] = isNullColumn[i] && allNull;
      }
    }
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < inputSchema.getFields().size(); i++) {
      if (!isNullColumn[i]) {
        fields.add(inputSchema.getFields().get(i));
      }
    }
    outputSchema = new Schema(fields);
    while (!data.isEmpty()) {
      VectorSchemaRoot current = data.poll();
      assert current.getFieldVectors().size() == isNullColumn.length;
      List<FieldVector> vectors = new ArrayList<>();
      for (int i = 0; i < isNullColumn.length; i++) {
        FieldVector vector = current.getVector(i);
        if (isNullColumn[i]) {
          vector.close();
        } else {
          vectors.add(vector);
        }
      }
      try (Batch output = Batch.of(VectorSchemaRoots.create(vectors, current.getRowCount()))) {
        offerResult(output);
      }
    }
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      throw new ComputeException(
          "RemoveNullColumnExecutor need to consume end before getOutputSchema");
    }
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return "RemoveNullColumn";
  }
}
