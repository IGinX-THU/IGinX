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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalSum extends UnaryAccumulation {

  public static final String NAME = "sum";

  public PhysicalSum() {
    super(NAME);
  }

  @Override
  protected SumAccumulator<? extends SumState> accumulate(
      BufferAllocator allocator, Field inputField) throws ComputeException {
    Field resultField = new Field(getResultFieldName(inputField), inputField.getFieldType(), null);
    switch (Schemas.minorTypeOf(inputField)) {
      case INT:
        return new SumAccumulator<>(
            allocator, inputField, resultField, IntSumState.class, IntSumState::new);
      case BIGINT:
        return new SumAccumulator<>(
            allocator, inputField, resultField, BigIntSumState.class, BigIntSumState::new);
      case FLOAT4:
        return new SumAccumulator<>(
            allocator, inputField, resultField, Float4SumState.class, Float4SumState::new);
      case FLOAT8:
        return new SumAccumulator<>(
            allocator, inputField, resultField, Float8SumState.class, Float8SumState::new);
      default:
        throw new NotAllowTypeException(this, Schemas.of(inputField), 0);
    }
  }

  protected class SumAccumulator<S extends SumState> extends UnaryAccumulator<S> {

    protected SumAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected abstract static class SumState extends UnaryState {}

  protected static class IntSumState extends SumState {

    private int sum = 0;

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
        }
      }
    }

    @Override
    public void evaluate(FieldWriter writer) {
      writer.writeInt(sum);
    }
  }

  protected static class BigIntSumState extends SumState {

    private long sum = 0;

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
        }
      }
    }

    @Override
    public void evaluate(FieldWriter writer) {
      writer.writeBigInt(sum);
    }
  }

  protected static class Float4SumState extends SumState {

    private float sum = 0;

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
        }
      }
    }

    @Override
    public void evaluate(FieldWriter writer) {
      writer.writeFloat4(sum);
    }
  }

  protected static class Float8SumState extends SumState {

    private double sum = 0;

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
        }
      }
    }

    @Override
    public void evaluate(FieldWriter writer) throws ComputeException {
      writer.writeFloat8(sum);
    }
  }
}
