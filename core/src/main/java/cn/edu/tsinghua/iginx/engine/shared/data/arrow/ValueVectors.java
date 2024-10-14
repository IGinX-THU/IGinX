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
package cn.edu.tsinghua.iginx.engine.shared.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.TransferPair;

public class ValueVectors {

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, T source, int valueCount) {
    TransferPair transferPair = source.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, valueCount);
    return (T) transferPair.getTo();
  }

  public static <T extends ValueVector> T slice(BufferAllocator allocator, T source) {
    return slice(allocator, source, source.getValueCount());
  }

  public static ValueVector create(BufferAllocator allocator, Types.MinorType returnType) {
    return returnType.getNewVector(Schemas.defaultField(returnType), allocator, null);
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T like(BufferAllocator allocator, T left) {
    T ret = (T) create(allocator, left.getMinorType());
    ret.setValueCount(left.getValueCount());
    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeIntersection(
      BufferAllocator allocator, T first, T second) {
    int valueCount = Math.min(first.getValueCount(), second.getValueCount());
    if (first.getMinorType() != second.getMinorType()) {
      throw new IllegalArgumentException("Cannot create intersection vector for different types");
    }
    T ret = (T) create(allocator, first.getMinorType());
    ret.setValueCount(first.getValueCount());

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf firstValidityBuffer = first.getValidityBuffer();
    ArrowBuf secondValidityBuffer = second.getValidityBuffer();
    int byteCount = BitVectorHelper.getValidityBufferSize(valueCount);
    BitVectors.and(retValidityBuffer, firstValidityBuffer, secondValidityBuffer, byteCount);

    return ret;
  }
}
