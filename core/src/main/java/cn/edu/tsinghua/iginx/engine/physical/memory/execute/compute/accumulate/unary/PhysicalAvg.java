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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class PhysicalAvg extends UnaryAccumulation {

  public static final String NAME = "avg";

  public PhysicalAvg() {
    super(NAME);
  }

  @Override
  protected AvgAccumulator<? extends AvgState> accumulate(
      BufferAllocator allocator, Field inputField) throws ComputeException {
    Field resultField =
        new Field(
            getResultFieldName(inputField),
            new FieldType(true, Types.MinorType.FLOAT8.getType(), null, inputField.getMetadata()),
            null);
    switch (Schemas.minorTypeOf(inputField)) {
      case INT:
        return new AvgAccumulator<>(
            allocator, inputField, resultField, IntAvgState.class, IntAvgState::new);
      case BIGINT:
        return new AvgAccumulator<>(
            allocator, inputField, resultField, BigIntAvgState.class, BigIntAvgState::new);
      case FLOAT4:
        return new AvgAccumulator<>(
            allocator, inputField, resultField, Float4AvgState.class, Float4AvgState::new);
      case FLOAT8:
        return new AvgAccumulator<>(
            allocator, inputField, resultField, Float8AvgState.class, Float8AvgState::new);
      default:
        throw new NotAllowTypeException(this, Schemas.of(inputField), 0);
    }
  }

  protected class AvgAccumulator<S extends AvgState> extends UnaryAccumulator<S> {

    protected AvgAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected abstract static class AvgState extends UnaryState {

    protected double sum = 0; // Sum of all non-null elements
    protected long count = 0; // Number of non-null elements

    public void evaluate(FieldWriter writer) throws ComputeException {
      if (count == 0) {
        writer.writeNull();
      } else {
        writer.writeFloat8(sum / count);
      }
    }
  }

  protected static class IntAvgState extends AvgState {

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof IntVector)) {
        throw new ComputeException(
            "InputVector is not IntVector, but " + inputVector.getClass().getSimpleName());
      }
      IntVector intVector = (IntVector) inputVector;
      int valueCount = intVector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        if (!intVector.isNull(i)) {
          sum += intVector.get(i);
          count++;
        }
      }
    }
  }

  protected static class BigIntAvgState extends AvgState {

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof BigIntVector)) {
        throw new ComputeException(
            "InputVector is not BigIntVector, but " + inputVector.getClass().getSimpleName());
      }
      BigIntVector bigIntVector = (BigIntVector) inputVector;
      int valueCount = bigIntVector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        if (!bigIntVector.isNull(i)) {
          sum += bigIntVector.get(i);
          count++;
        }
      }
    }
  }

  protected static class Float4AvgState extends AvgState {

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof Float4Vector)) {
        throw new ComputeException(
            "InputVector is not Float4Vector, but " + inputVector.getClass().getSimpleName());
      }
      Float4Vector float4Vector = (Float4Vector) inputVector;
      int valueCount = float4Vector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        if (!float4Vector.isNull(i)) {
          sum += float4Vector.get(i);
          count++;
        }
      }
    }
  }

  protected static class Float8AvgState extends AvgState {

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof Float8Vector)) {
        throw new ComputeException(
            "InputVector is not Float8Vector, but " + inputVector.getClass().getSimpleName());
      }
      Float8Vector float8Vector = (Float8Vector) inputVector;
      int valueCount = float8Vector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        if (!float8Vector.isNull(i)) {
          sum += float8Vector.get(i);
          count++;
        }
      }
    }
  }
}
