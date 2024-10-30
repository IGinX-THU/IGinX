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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CallContracts;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public abstract class UnaryLogicFunction extends UnaryFunction<BitVector> {

  protected UnaryLogicFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      @WillNotClose BufferAllocator allocator, @WillNotClose FieldVector input)
      throws NotAllowTypeException {
    CallContracts.ensureType(this, Schemas.of(input), Types.MinorType.BIT);
    return evaluate(allocator, (BitVector) input);
  }

  public BitVector evaluate(
      @WillNotClose BufferAllocator allocator, @WillNotClose BitVector input) {
    int rowCount = input.getValueCount();
    BitVector result = (BitVector) ValueVectors.create(allocator, Types.MinorType.BIT, rowCount);
    int byteCount = BitVectorHelper.getValidityBufferSize(rowCount);
    evaluate(result.getDataBuffer(), input.getDataBuffer(), byteCount);
    return result;
  }

  public void evaluate(
      @WillNotClose ArrowBuf result, @WillNotClose ArrowBuf input, long byteCount) {
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
