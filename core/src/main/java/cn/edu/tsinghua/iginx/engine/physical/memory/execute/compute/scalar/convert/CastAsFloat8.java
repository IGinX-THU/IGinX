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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;

class CastAsFloat8 extends AbstractCast {

  public CastAsFloat8() {
    super(Types.MinorType.FLOAT8);
  }

  @Override
  public FieldVector evaluate(BufferAllocator allocator, FieldVector input)
      throws ComputeException {
    switch (input.getMinorType()) {
      case INT:
        return evaluate(allocator, (IntVector) input);
      case BIGINT:
        return evaluate(allocator, (BigIntVector) input);
      default:
        throw new NotAllowArgumentTypeException(this, 0, input.getMinorType());
    }
  }

  private Float8Vector evaluate(BufferAllocator allocator, IntVector input) {
    // TODO: 使用工具函数创建
    Float8Vector res = new Float8Vector(input.getName(), allocator);
    res.allocateNew(input.getValueCount());
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!input.isNull(i)) {
        res.setSafe(i, input.get(i));
      } else {
        res.setNull(i);
      }
    }
    res.setValueCount(input.getValueCount());
    return res;
  }

  private Float8Vector evaluate(BufferAllocator allocator, BigIntVector input) {
    // TODO: 使用工具函数创建
    Float8Vector res = new Float8Vector(input.getName(), allocator);
    res.allocateNew(input.getValueCount());
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!input.isNull(i)) {
        res.setSafe(i, (double) input.get(i));
      } else {
        res.setNull(i);
      }
    }
    res.setValueCount(input.getValueCount());
    return res;
  }
}
