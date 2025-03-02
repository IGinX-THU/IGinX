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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class PhysicalCount extends UnaryAccumulation {

  public static final String NAME = "count";

  public PhysicalCount() {
    super(NAME);
  }

  @Override
  protected CountAccumulator accumulate(BufferAllocator allocator, Field inputField)
      throws ComputeException {
    Field resultField =
        new Field(
            getResultFieldName(inputField),
            new FieldType(true, Types.MinorType.BIGINT.getType(), null, inputField.getMetadata()),
            null);
    return new CountAccumulator(allocator, inputField, resultField);
  }

  protected class CountAccumulator extends UnaryAccumulator<CountState> {
    protected CountAccumulator(BufferAllocator allocator, Field inputField, Field outpuField) {
      super(allocator, inputField, outpuField, CountState.class, CountState::new);
    }
  }

  protected static class CountState extends UnaryState {

    protected long count = 0;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      count += inputVector.getValueCount() - inputVector.getNullCount();
    }

    @Override
    public void evaluate(FieldWriter writer) throws ComputeException {
      writer.writeBigInt(count);
    }
  }
}
