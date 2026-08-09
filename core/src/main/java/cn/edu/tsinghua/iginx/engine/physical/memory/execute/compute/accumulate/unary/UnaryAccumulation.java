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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.AbstractAccumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class UnaryAccumulation extends AbstractAccumulation {

  protected UnaryAccumulation(String name) {
    super(name, Arity.UNARY);
  }

  @Override
  protected AbstractAccumulator<? extends AbstractState> accumulateImpl(
      BufferAllocator allocator, Schema inputSchema) throws ComputeException {
    Field inputField = inputSchema.getFields().get(0);
    return accumulate(allocator, inputField);
  }

  protected String getResultFieldName(Field inputField) {
    return getName() + "(" + inputField.getName() + ")";
  }

  protected abstract UnaryAccumulator<? extends UnaryState> accumulate(
      BufferAllocator allocator, Field inputField) throws ComputeException;

  protected class UnaryAccumulator<S extends UnaryState> extends AbstractAccumulator<S> {

    protected final Field field;

    protected UnaryAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outputField,
        Class<? extends S> stateClass,
        Supplier<S> factory) {
      super(allocator, Schemas.of(inputField), outputField, stateClass, factory);
      this.field = inputField;
    }
  }

  protected abstract static class UnaryState extends AbstractState {
    @Override
    public void update(VectorSchemaRoot root) throws ComputeException {
      FieldVector inputVector = root.getVector(0);
      update(inputVector);
    }

    public abstract void update(FieldVector inputVector) throws ComputeException;
  }
}
