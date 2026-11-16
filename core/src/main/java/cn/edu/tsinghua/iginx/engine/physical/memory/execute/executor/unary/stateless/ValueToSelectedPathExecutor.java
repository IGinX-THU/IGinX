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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless;

import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.Collections;
import java.util.Objects;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ValueToSelectedPathExecutor extends StatelessUnaryExecutor {

  private final String prefix;
  public static final Field SELECTED_PATH_FIELD =
      Field.notNullable("SelectedPath", Types.MinorType.VARBINARY.getType());
  private static final Schema OUTPUT_SCHEMA =
      new Schema(Collections.singletonList(SELECTED_PATH_FIELD));

  public ValueToSelectedPathExecutor(ExecutorContext context, Schema inputSchema, String prefix) {
    super(context, inputSchema);
    this.prefix = Objects.requireNonNull(prefix);
  }

  @Override
  protected Batch computeImpl(Batch batch) {
    VarBinaryVector selectedPathVector =
        (VarBinaryVector) SELECTED_PATH_FIELD.createVector(context.getAllocator());
    int selectedPathCount = 0;

    FieldVector[] inputVectors = batch.getVectors().toArray(new FieldVector[0]);
    int rowCount = batch.getRowCount();
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      for (FieldVector inputVector : inputVectors) {
        Object value = inputVector.getObject(rowIndex);
        if (value == null) {
          continue;
        }
        String suffix;
        if (value instanceof byte[]) {
          suffix = new String((byte[]) value);
          if (suffix.isEmpty()) {
            continue;
          }
        } else {
          suffix = value.toString();
        }
        String path = prefix.isEmpty() ? suffix : prefix + DOT + suffix;
        byte[] pathBytes = path.getBytes();
        selectedPathVector.setSafe(selectedPathCount++, pathBytes);
      }
    }
    selectedPathVector.setValueCount(selectedPathCount);
    return Batch.of(new VectorSchemaRoot(Collections.singletonList(selectedPathVector)));
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return OUTPUT_SCHEMA;
  }

  @Override
  protected String getInfo() {
    return "ValueToSelectedPath(prefix=" + prefix + ")";
  }

  @Override
  public void close() throws ComputeException {}
}
