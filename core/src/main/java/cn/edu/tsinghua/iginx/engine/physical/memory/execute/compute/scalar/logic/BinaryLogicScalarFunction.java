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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.BinaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CallContracts;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public abstract class BinaryLogicScalarFunction extends BinaryScalarFunction<BitVector> {

  protected BinaryLogicScalarFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      BufferAllocator allocator,
      @Nullable BaseIntVector selection,
      FieldVector left,
      FieldVector right)
      throws ComputeException {
    if (selection == null) {
      return evaluate(allocator, left, right);
    }
    // TODO: implement this in subclasses
    try (FieldVector leftSelected = PhysicalFunctions.take(allocator, selection, left);
        FieldVector rightSelected = PhysicalFunctions.take(allocator, selection, right)) {
      return evaluate(allocator, leftSelected, rightSelected);
    }
  }

  public BitVector evaluate(BufferAllocator allocator, FieldVector left, FieldVector right)
      throws NotAllowTypeException {
    CallContracts.ensureType(this, Schemas.of(left, right), Types.MinorType.BIT);
    return evaluate(allocator, (BitVector) left, (BitVector) right);
  }

  public BitVector evaluate(BufferAllocator allocator, BitVector left, BitVector right) {
    int rowCount = Math.min(left.getValueCount(), right.getValueCount());
    BitVector result = (BitVector) ValueVectors.create(allocator, Types.MinorType.BIT, rowCount);
    int byteCount = BitVectorHelper.getValidityBufferSize(rowCount);
    new And()
        .evaluate(
            result.getValidityBuffer(),
            left.getValidityBuffer(),
            right.getValidityBuffer(),
            byteCount);
    evaluate(result.getDataBuffer(), left.getDataBuffer(), right.getDataBuffer(), byteCount);
    return result;
  }

  public void evaluate(ArrowBuf result, ArrowBuf left, ArrowBuf right, long byteCount) {
    if (result.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of result buffer is not enough");
    }
    if (left.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of left buffer is not enough");
    }
    if (right.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of right buffer is not enough");
    }
    long countAligned = (byteCount / Long.BYTES) * Long.BYTES;
    for (long index = 0; index < countAligned; index += Long.BYTES) {
      result.setLong(index, evaluate(left.getLong(index), right.getLong(index)));
    }
    for (long index = countAligned; index < byteCount; index++) {
      result.setByte(index, evaluate(left.getByte(index), right.getByte(index)));
    }
  }

  public abstract byte evaluate(byte left, byte right);

  public abstract long evaluate(long left, long right);
}
