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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.select;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.types.Types;

public class VectorFilter extends BinaryFunction {

  public static final String NAME = "filter";

  public VectorFilter() {
    super(NAME);
  }

  @Override
  public FieldVector evaluate(
      @WillNotClose BufferAllocator allocator,
      @WillNotClose FieldVector left,
      @WillNotClose FieldVector right)
      throws ComputeException {
    if (left instanceof NullVector || right instanceof NullVector) {
      // TODO: give type for null vector
      return ConstantVectors.ofNull(
          allocator, Math.min(left.getValueCount(), right.getValueCount()));
    }
    if (left.getMinorType() != Types.MinorType.BIT) {
      throw new NotAllowArgumentTypeException(this, 0, left.getMinorType());
    }
    return evaluate(allocator, (BitVector) left, right);
  }

  public <T extends FieldVector> FieldVector evaluate(
      @WillNotClose BufferAllocator allocator, BitVector bitVector, T fieldVector) {
    T result = ValueVectors.like(allocator, fieldVector);
    int targetIndex = 0;
    for (int sourceIndex = 0; sourceIndex < bitVector.getValueCount(); sourceIndex++) {
      if (!bitVector.isNull(sourceIndex) && bitVector.get(sourceIndex) != 0) {
        result.copyFrom(sourceIndex, targetIndex, fieldVector);
        targetIndex++;
      }
    }
    result.setValueCount(targetIndex);
    return result;
  }
}
