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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.AbstractAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class UnaryAccumulator extends AbstractAccumulator {

  protected UnaryAccumulator(String name) {
    super(name, Arity.UNARY);
  }

  @Override
  protected AbstractState initializeImpl(BufferAllocator allocator, Schema schema)
      throws ComputeException {
    if (schema.getFields().size() != 1) {
      throw new ComputeException(
          "Unary function should have only one argument, but got " + schema.getFields().size());
    }
    Field field = schema.getFields().get(0);
    return initializeImpl(allocator, field);
  }

  protected abstract UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException;

  protected abstract static class UnaryState extends AbstractState {

    protected final Field field;

    protected UnaryState(BufferAllocator allocator, Field field, UnaryAccumulator accumulator) {
      super(allocator, new Schema(Collections.singletonList(field)), accumulator);
      this.field = field;
    }

    @Override
    protected void accumulateImpl(VectorSchemaRoot root) throws ComputeException {
      if (root.getFieldVectors().size() != 1) {
        throw new ComputeException(
            "Unary function should have only one argument, but got "
                + root.getFieldVectors().size());
      }
      ValueVector vector = root.getVector(0);
      if (vector instanceof NullVector) {
        return;
      }
      accumulateImpl(vector);
    }

    @Override
    public FieldVector evaluate() throws ComputeException {
      try (FieldVector vector = evaluateImpl()) {
        Field oldField = vector.getField();
        String resultName = accumulator.getName() + "(" + oldField.getName() + ")";
        return ValueVectors.transfer(allocator, vector, resultName);
      }
    }

    protected abstract void accumulateImpl(ValueVector vector) throws ComputeException;

    protected abstract FieldVector evaluateImpl() throws ComputeException;
  }
}
