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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class Hash extends AbstractFunction<IntVector> {

  public Hash() {
    super("hash", Arity.ANY);
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass() == this.getClass();
  }

  @Override
  protected IntVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    String name =
        input.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.joining(",", getName() + "(", ")"));
    FieldType fieldType = FieldType.notNullable(Types.MinorType.INT.getType());
    IntVector output = new IntVector(name, fieldType, allocator);
    output.allocateNew(input.getRowCount());
    ConstantVectors.setValueCountWithValidity(output, input.getRowCount());

    List<FieldVector> args = input.getFieldVectors();
    if (!args.isEmpty()) {
      FieldVector first = args.get(0);
      for (int rowIndex = 0; rowIndex < input.getRowCount(); rowIndex++) {
        set(output, rowIndex, first.hashCode(rowIndex));
      }
    }
    for (int columnIndex = 1; columnIndex < args.size(); columnIndex++) {
      FieldVector fieldVector = args.get(columnIndex);
      for (int rowIndex = 0; rowIndex < input.getRowCount(); rowIndex++) {
        int hash = 31 * output.hashCode(rowIndex) + fieldVector.hashCode(rowIndex);
        set(output, rowIndex, hash);
      }
    }
    return output;
  }

  private static void set(IntVector vector, int index, int value) {
    vector.getDataBuffer().setInt((long) index * IntVector.TYPE_WIDTH, value);
  }
}
