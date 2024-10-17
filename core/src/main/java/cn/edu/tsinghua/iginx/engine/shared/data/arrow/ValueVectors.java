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
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
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

  public static FieldVector create(BufferAllocator allocator, Field field) {
    return field.createVector(allocator);
  }

  public static FieldVector create(
      BufferAllocator allocator, Types.MinorType minorType, int rowCount) {
    FieldVector ret = create(allocator, minorType);
    ret.setInitialCapacity(rowCount);
    ret.setValueCount(rowCount);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeOnlyField(BufferAllocator allocator, T left) {
    return (T) create(allocator, left.getField());
  }

  public static <T extends ValueVector> T like(BufferAllocator allocator, T left) {
    T ret = likeOnlyField(allocator, left);
    ret.setInitialCapacity(left.getValueCount());
    ret.setValueCount(left.getValueCount());
    return ret;
  }

  public static ValueVector createWithBothValidity(
      BufferAllocator allocator, ValueVector left, ValueVector right, Types.MinorType type) {
    ValueVector ret = create(allocator, type);

    int valueCount = Math.min(left.getValueCount(), right.getValueCount());
    ret.setInitialCapacity(valueCount);
    ret.setValueCount(valueCount);

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf firstValidityBuffer = left.getValidityBuffer();
    ArrowBuf secondValidityBuffer = right.getValidityBuffer();

    new And()
        .evaluate(
            retValidityBuffer,
            firstValidityBuffer,
            secondValidityBuffer,
            BitVectorHelper.getValidityBufferSize(valueCount));
    return ret;
  }

  public static ValueVector createWithValidity(
      BufferAllocator allocator, ValueVector input, Types.MinorType type) {
    ValueVector ret = create(allocator, type);

    int valueCount = input.getValueCount();
    ret.setInitialCapacity(valueCount);
    ret.setValueCount(valueCount);

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf inputValidityBuffer = input.getValidityBuffer();
    long capacity = Math.min(retValidityBuffer.capacity(), inputValidityBuffer.capacity());
    MemoryUtil.UNSAFE.copyMemory(
        retValidityBuffer.memoryAddress(), inputValidityBuffer.memoryAddress(), capacity);

    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeWithBothValidity(
      BufferAllocator allocator, T first, T second) {
    if (first.getMinorType() != second.getMinorType()) {
      throw new IllegalArgumentException("Cannot create intersection vector for different types");
    }
    return (T) createWithBothValidity(allocator, first, second, first.getMinorType());
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeWithValidity(BufferAllocator allocator, T vector) {
    return (T) createWithValidity(allocator, vector, vector.getMinorType());
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
