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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public abstract class AbstractScalarCast<OUTPUT extends FieldVector>
    extends UnaryScalarFunction<OUTPUT> {

  protected final Types.MinorType resultType;
  protected final boolean isNullable;

  protected AbstractScalarCast(Types.MinorType resultType) {
    this(resultType, true);
  }

  protected AbstractScalarCast(Types.MinorType resultType, boolean isNullable) {
    super("cast_as_" + resultType.toString().toLowerCase());
    this.resultType = resultType;
    this.isNullable = isNullable;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    return obj != null && getClass() == obj.getClass();
  }

  @Override
  public OUTPUT evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException {
    if (selection == null) {
      return evaluate(allocator, input);
    }
    try (FieldVector selected = PhysicalFunctions.take(allocator, selection, input)) {
      return evaluate(allocator, selected);
    }
  }

  @SuppressWarnings("unchecked")
  public OUTPUT evaluate(BufferAllocator allocator, FieldVector input) throws ComputeException {
    if (input.getMinorType() == resultType) {
      if (input.getField().isNullable() == isNullable) {
        return ValueVectors.slice(allocator, (OUTPUT) input);
      } else if (isNullable) {
        return ValueVectors.slice(allocator, (OUTPUT) input, true);
      }
    }

    Field inputField = input.getField();
    Field outField =
        new Field(
            inputField.getName(),
            new FieldType(
                isNullable,
                resultType.getType(),
                inputField.getDictionary(),
                inputField.getMetadata()),
            inputField.getChildren());

    try (OUTPUT dest = (OUTPUT) outField.createVector(allocator)) {
      evaluate(dest, input);
      return ValueVectors.transfer(allocator, dest);
    }
  }

  protected void evaluate(OUTPUT dest, FieldVector input) throws ComputeException {
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

  protected void fail(FieldVector input) throws NotAllowTypeException {}

  protected void evaluate(OUTPUT dest, BitVector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException(
              "Cannot cast boolean vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i) != 0);
    }
  }

  protected void evaluate(OUTPUT dest, IntVector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException("Cannot cast int vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i));
    }
  }

  protected void evaluate(OUTPUT dest, BigIntVector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException("Cannot cast long vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i));
    }
  }

  protected void evaluate(OUTPUT dest, Float4Vector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException("Cannot cast float vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i));
    }
  }

  protected void evaluate(OUTPUT dest, Float8Vector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException(
              "Cannot cast double vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i));
    }
  }

  protected void evaluate(OUTPUT dest, VarBinaryVector input) throws ComputeException {
    boolean inputNullable = input.getField().isNullable();
    for (int i = 0; i < input.getValueCount(); i++) {
      if (inputNullable && input.isNull(i)) {
        if (!isNullable) {
          throw new ComputeException(
              "Cannot cast binary vector contains null to non-nullable type");
        }
        dest.setNull(i);
        continue;
      }
      set(dest, i, input.get(i));
    }
  }

  protected void set(OUTPUT dest, int index, boolean value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("boolean", Types.MinorType.BIT.getType())), 0);
  }

  protected void set(OUTPUT dest, int index, int value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("int", Types.MinorType.INT.getType())), 0);
  }

  protected void set(OUTPUT dest, int index, long value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("long", Types.MinorType.BIGINT.getType())), 0);
  }

  protected void set(OUTPUT dest, int index, float value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("float", Types.MinorType.FLOAT4.getType())), 0);
  }

  protected void set(OUTPUT dest, int index, double value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("double", Types.MinorType.FLOAT8.getType())), 0);
  }

  protected void set(OUTPUT dest, int index, byte[] value) throws ComputeException {
    throw new NotAllowTypeException(
        this, Schemas.of(Field.nullable("binary", Types.MinorType.VARBINARY.getType())), 0);
  }
}
