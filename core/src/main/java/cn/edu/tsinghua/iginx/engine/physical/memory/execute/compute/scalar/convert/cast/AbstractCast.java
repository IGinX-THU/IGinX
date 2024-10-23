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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public abstract class AbstractCast<VEC extends FieldVector> extends UnaryFunction {

  protected final Types.MinorType resultType;

  protected AbstractCast(Types.MinorType resultType) {
    super("cast_as_" + resultType.toString().toLowerCase());
    this.resultType = resultType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public VEC evaluate(@WillNotClose BufferAllocator allocator, @WillNotClose FieldVector input)
      throws ComputeException {
    if (input.getMinorType() == resultType) {
      return ValueVectors.slice(allocator, (VEC) input);
    }

    Field inputField = input.getField();
    Field outField =
        new Field(
            inputField.getName(),
            new FieldType(
                inputField.isNullable(),
                resultType.getType(),
                inputField.getDictionary(),
                inputField.getMetadata()),
            inputField.getChildren());

    try (VEC dest = (VEC) outField.createVector(allocator)) {
      evaluate(dest, input);
      return ValueVectors.transfer(allocator, dest);
    }
  }

  protected void evaluate(VEC dest, FieldVector input) throws ComputeException {
    if (dest instanceof FixedWidthVector) {
      ((FixedWidthVector) dest).allocateNew(input.getValueCount());
    } else {
      dest.setInitialCapacity(input.getValueCount());
    }
    switch (input.getMinorType()) {
      case BIT:
        evaluate(dest, (BitVector) input);
        break;
      case INT:
        evaluate(dest, (IntVector) input);
        break;
      case BIGINT:
        evaluate(dest, (BigIntVector) input);
        break;
      case FLOAT4:
        evaluate(dest, (Float4Vector) input);
        break;
      case FLOAT8:
        evaluate(dest, (Float8Vector) input);
        break;
      case VARBINARY:
        evaluate(dest, (VarBinaryVector) input);
        break;
      default:
        fail(input);
    }
    dest.setValueCount(input.getValueCount());
  }

  protected void fail(FieldVector input) throws NotAllowTypeException {
    throw new NotAllowTypeException(this, Schemas.of(input), 0);
  }

  protected void evaluate(VEC dest, BitVector input) throws ComputeException {
    fail(input);
  }

  protected void evaluate(VEC dest, IntVector input) throws ComputeException {
    fail(input);
  }

  protected void evaluate(VEC dest, BigIntVector input) throws ComputeException {
    fail(input);
  }

  protected void evaluate(VEC dest, Float4Vector input) throws ComputeException {
    fail(input);
  }

  protected void evaluate(VEC dest, Float8Vector input) throws ComputeException {
    fail(input);
  }

  protected void evaluate(VEC dest, VarBinaryVector input) throws ComputeException {
    fail(input);
  }
}
