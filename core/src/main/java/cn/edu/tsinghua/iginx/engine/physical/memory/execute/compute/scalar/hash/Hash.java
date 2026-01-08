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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.AbstractScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class Hash extends AbstractScalarFunction<IntVector> {

  public Hash() {
    super("hash", Arity.ANY);
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass() == this.getClass();
  }

  @Override
  protected IntVector invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (selection == null) {
      return invokeImpl(allocator, input);
    }
    try (VectorSchemaRoot selected = PhysicalFunctions.take(allocator, selection, input)) {
      return invokeImpl(allocator, selected);
    }
  }

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

    ArrowBuf data = output.getDataBuffer();
    FieldVector[] args = input.getFieldVectors().toArray(new FieldVector[0]);
    for (int rowIndex = 0; rowIndex < input.getRowCount(); rowIndex++) {
      int hash = 0;
      for (FieldVector arg : args) {
        hash = 31 * hash + arg.hashCode(rowIndex);
      }
      data.setInt((long) rowIndex * IntVector.TYPE_WIDTH, hash);
    }
    return output;
  }
}
