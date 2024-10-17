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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalSum extends UnaryAccumulator {

  public static final String NAME = "sum";

  public PhysicalSum() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntSumState(allocator, field, this);
      case BIGINT:
        return new BigIntSumState(allocator, field, this);
      case FLOAT4:
        return new Float4SumState(allocator, field, this);
      case FLOAT8:
        return new Float8SumState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class SumState extends UnaryState {

    protected SumState(BufferAllocator allocator, Field field, PhysicalSum accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    public boolean needMoreData() throws ComputeException {
      return true;
    }

    @Override
    public void close() throws ComputeException {}

    @Override
    protected FieldVector evaluateImpl() throws ComputeException {
      FieldVector result = field.createVector(allocator);
      writeValue(result.getMinorType().getNewFieldWriter(result));
      return result;
    }

    protected abstract void writeValue(FieldWriter writer);
  }

  protected static class IntSumState extends SumState {

    private int sum = 0;

    public IntSumState(BufferAllocator allocator, Field field, PhysicalSum accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      for (int i = 0; i < intVector.getValueCount(); i++) {
        if (!intVector.isNull(i)) {
          sum += intVector.get(i);
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeInt(sum);
    }
  }

  protected static class BigIntSumState extends SumState {

    private long sum = 0;

    public BigIntSumState(BufferAllocator allocator, Field field, PhysicalSum accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      for (int i = 0; i < bigIntVector.getValueCount(); i++) {
        if (!bigIntVector.isNull(i)) {
          sum += bigIntVector.get(i);
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBigInt(sum);
    }
  }

  protected static class Float4SumState extends SumState {

    private float sum = 0;

    public Float4SumState(BufferAllocator allocator, Field field, PhysicalSum accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      for (int i = 0; i < float4Vector.getValueCount(); i++) {
        if (!float4Vector.isNull(i)) {
          sum += float4Vector.get(i);
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat4(sum);
    }
  }

  protected static class Float8SumState extends SumState {

    private double sum = 0;

    public Float8SumState(BufferAllocator allocator, Field field, PhysicalSum accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      for (int i = 0; i < float8Vector.getValueCount(); i++) {
        if (!float8Vector.isNull(i)) {
          sum += float8Vector.get(i);
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat8(sum);
    }
  }
}
