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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.And;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.TransferPair;

public class ValueVectors {

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, T source, int valueCount, String ref) {
    TransferPair transferPair = source.getTransferPair(ref, allocator);
    transferPair.splitAndTransfer(0, valueCount);
    return (T) transferPair.getTo();
  }

  public static <T extends ValueVector> T slice(BufferAllocator allocator, T source, String ref) {
    return slice(allocator, source, source.getValueCount(), ref);
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, T source, int valueCount) {
    return slice(allocator, source, valueCount, source.getName());
  }

  public static <T extends ValueVector> T slice(BufferAllocator allocator, T source) {
    return slice(allocator, source, source.getValueCount());
  }

  public static FieldVector create(BufferAllocator allocator, Types.MinorType returnType) {
    return returnType.getNewVector(Schemas.defaultField(returnType), allocator, null);
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeType(BufferAllocator allocator, T left) {
    return (T) create(allocator, left.getMinorType());
  }

  public static <T extends ValueVector> T like(BufferAllocator allocator, T left) {
    T ret = likeType(allocator, left);
    ret.setValueCount(left.getValueCount());
    return ret;
  }

  public static ValueVector createIntersect(BufferAllocator allocator, ValueVector left, ValueVector right, Types.MinorType type) {
    ValueVector ret = create(allocator, type);

    int valueCount = Math.min(left.getValueCount(), right.getValueCount());
    ret.setInitialCapacity(valueCount);
    ret.setValueCount(valueCount);

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf firstValidityBuffer = left.getValidityBuffer();
    ArrowBuf secondValidityBuffer = right.getValidityBuffer();
    try (And and = new And()) {
      and.evaluate(retValidityBuffer, firstValidityBuffer, secondValidityBuffer);
    }

    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeIntersection(
      BufferAllocator allocator, T first, T second) {
    if (first.getMinorType() != second.getMinorType()) {
      throw new IllegalArgumentException("Cannot create intersection vector for different types");
    }
    return (T) createIntersect(allocator, first, second, first.getMinorType());
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T transfer(
      BufferAllocator allocator, T result, String ref) {
    TransferPair transferPair = result.getTransferPair(ref, allocator);
    transferPair.transfer();
    return (T) transferPair.getTo();
  }

  public static FieldVector transfer(BufferAllocator allocator, FieldVector vector) {
    return transfer(allocator, vector, vector.getName());
  }
}
