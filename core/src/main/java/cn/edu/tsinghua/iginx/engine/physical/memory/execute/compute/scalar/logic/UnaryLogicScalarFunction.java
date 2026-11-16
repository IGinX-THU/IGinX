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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CallContracts;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantVectors;
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

public abstract class UnaryLogicScalarFunction extends UnaryScalarFunction<BitVector> {

  protected UnaryLogicScalarFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector in)
      throws ComputeException {
    if (selection == null) {
      return evaluate(allocator, in);
    }
    try (FieldVector selected = PhysicalFunctions.take(allocator, selection, in)) {
      return evaluate(allocator, selected);
    }
  }

  public BitVector evaluate(BufferAllocator allocator, FieldVector input)
      throws NotAllowTypeException {
    CallContracts.ensureType(this, Schemas.of(input), Types.MinorType.BIT);
    return evaluate(allocator, (BitVector) input);
  }

  public BitVector evaluate(BufferAllocator allocator, BitVector input) {
    int rowCount = input.getValueCount();
    BitVector result = (BitVector) ValueVectors.create(allocator, Types.MinorType.BIT, rowCount);
    ConstantVectors.setAllValidity(result, rowCount);
    int byteCount = BitVectorHelper.getValidityBufferSize(rowCount);
    evaluate(result.getDataBuffer(), input.getDataBuffer(), byteCount);
    return result;
  }

  public void evaluate(ArrowBuf result, ArrowBuf input, long byteCount) {
    if (result.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of result buffer is not enough");
    }
    if (input.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of input buffer is not enough");
    }
    long countAligned = (byteCount / Long.BYTES) * Long.BYTES;
    for (long index = 0; index < countAligned; index += Long.BYTES) {
      result.setLong(index, evaluate(input.getLong(index)));
    }
    for (long index = countAligned; index < byteCount; index++) {
      result.setByte(index, evaluate(input.getByte(index)));
    }
  }

  public abstract byte evaluate(byte input);

  public abstract long evaluate(long input);
}
