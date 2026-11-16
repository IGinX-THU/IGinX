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
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;

public abstract class PhysicalSelect extends UnaryAccumulation {

  protected PhysicalSelect(String name) {
    super(name);
  }

  @Override
  protected SelectAccumulator<? extends SelectState> accumulate(
      BufferAllocator allocator, Field inputField) throws ComputeException {
    Field resultField = Schemas.fieldWithName(inputField, getResultFieldName(inputField));
    switch (Schemas.minorTypeOf(inputField)) {
      case BIT:
        return accumulateBit(allocator, inputField, resultField);
      case INT:
        return accumulateInt(allocator, inputField, resultField);
      case BIGINT:
        return accumulateBigInt(allocator, inputField, resultField);
      case FLOAT4:
        return accumulateFloat4(allocator, inputField, resultField);
      case FLOAT8:
        return accumulateFloat8(allocator, inputField, resultField);
      case VARBINARY:
        return accumulateVarBinary(allocator, inputField, resultField);
      default:
        throw new NotAllowTypeException(this, Schemas.of(inputField), 0);
    }
  }

  protected abstract SelectAccumulator<? extends BitSelectState> accumulateBit(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected abstract SelectAccumulator<? extends IntSelectState> accumulateInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected abstract SelectAccumulator<? extends BigIntSelectState> accumulateBigInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected abstract SelectAccumulator<? extends Float4SelectState> accumulateFloat4(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected abstract SelectAccumulator<? extends Float8SelectState> accumulateFloat8(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected abstract SelectAccumulator<? extends VarBinarySelectState> accumulateVarBinary(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException;

  protected class SelectAccumulator<S extends SelectState> extends UnaryAccumulator<S> {

    protected SelectAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected abstract static class SelectState extends UnaryState {

    protected boolean found = false;

    protected void genericUpdate(
        FieldVector vector, IntConsumer firstFoundConsumer, IntConsumer consumer) {
      int valueCount = vector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          if (found) {
            consumer.accept(i);
          } else {
            firstFoundConsumer.accept(i);
            found = true;
          }
        }
      }
    }

    protected void genericWriteResult(FieldWriter writer, Runnable writeValue) {
      if (found) {
        writeValue.run();
      } else {
        writer.writeNull();
      }
    }
  }

  protected abstract static class BitSelectState extends SelectState {

    private boolean value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof BitVector)) {
        throw new ComputeException(
            "InputVector is not BitVector, but " + inputVector.getClass().getSimpleName());
      }
      BitVector bitVector = (BitVector) inputVector;
      genericUpdate(
          inputVector,
          i -> value = bitVector.get(i) == 1,
          i -> value = select(value, bitVector.get(i) == 1));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeBit(value ? 1 : 0));
    }

    protected abstract boolean select(boolean newValue, boolean oldValue);
  }

  protected abstract static class IntSelectState extends SelectState {

    private int value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof IntVector)) {
        throw new ComputeException(
            "InputVector is not IntVector, but " + inputVector.getClass().getSimpleName());
      }
      IntVector intVector = (IntVector) inputVector;
      genericUpdate(
          inputVector, i -> value = intVector.get(i), i -> value = select(value, intVector.get(i)));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeInt(value));
    }

    protected abstract int select(int newValue, int oldValue);
  }

  protected abstract static class BigIntSelectState extends SelectState {

    private long value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof BigIntVector)) {
        throw new ComputeException(
            "InputVector is not BigIntVector, but " + inputVector.getClass().getSimpleName());
      }
      BigIntVector bigIntVector = (BigIntVector) inputVector;
      genericUpdate(
          inputVector,
          i -> value = bigIntVector.get(i),
          i -> value = select(value, bigIntVector.get(i)));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeBigInt(value));
    }

    protected abstract long select(long newValue, long oldValue);
  }

  protected abstract static class Float4SelectState extends SelectState {

    private float value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof Float4Vector)) {
        throw new ComputeException(
            "InputVector is not Float4Vector, but " + inputVector.getClass().getSimpleName());
      }
      Float4Vector float4Vector = (Float4Vector) inputVector;
      genericUpdate(
          inputVector,
          i -> value = float4Vector.get(i),
          i -> value = select(value, float4Vector.get(i)));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeFloat4(value));
    }

    protected abstract float select(float newValue, float oldValue);
  }

  protected abstract static class Float8SelectState extends SelectState {

    private double value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof Float8Vector)) {
        throw new ComputeException(
            "InputVector is not Float8Vector, but " + inputVector.getClass().getSimpleName());
      }
      Float8Vector float8Vector = (Float8Vector) inputVector;
      genericUpdate(
          inputVector,
          i -> value = float8Vector.get(i),
          i -> value = select(value, float8Vector.get(i)));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeFloat8(value));
    }

    protected abstract double select(double newValue, double oldValue);
  }

  protected abstract static class VarBinarySelectState extends SelectState {

    private byte[] value;

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      if (!(inputVector instanceof VarBinaryVector)) {
        throw new ComputeException(
            "InputVector is not VarBinaryVector, but " + inputVector.getClass().getSimpleName());
      }
      VarBinaryVector varBinaryVector = (VarBinaryVector) inputVector;
      genericUpdate(
          inputVector,
          i -> value = varBinaryVector.get(i),
          i -> value = select(value, varBinaryVector.get(i)));
    }

    @Override
    public void evaluate(FieldWriter writer) {
      genericWriteResult(writer, () -> writer.writeVarBinary(value));
    }

    protected abstract byte[] select(byte[] newValue, byte[] oldValue);
  }
}
