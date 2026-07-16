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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.string;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;

public class BinarySlice extends UnaryScalarFunction<VarBinaryVector> {

  public static final String NAME = "binary_slice";

  private final int start;
  private final int length;

  public BinarySlice(int start, int length) {
    super(NAME + "<" + start + "," + length + ">");
    Preconditions.checkArgument(start >= 0, "start must be non-negative");
    Preconditions.checkArgument(length >= 0, "length must be non-negative");
    this.start = start;
    this.length = length;
  }

  @Override
  public VarBinaryVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException {
    if (input instanceof VarBinaryVector) {
      return evaluate(allocator, selection, (VarBinaryVector) input);
    }
    throw new NotAllowTypeException(this, Schemas.of(input), 0);
  }

  public VarBinaryVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VarBinaryVector input)
      throws ComputeException {
    int destRowCount = selection == null ? input.getValueCount() : selection.getValueCount();
    VarBinaryVector dest = (VarBinaryVector) input.getField().createVector(allocator);
    dest.allocateNew((long) destRowCount * length, destRowCount);

    ArrowBufPointer valuePointer = new ArrowBufPointer();
    for (int destIndex = 0; destIndex < destRowCount; destIndex++) {
      int sourceIndex = destIndex;
      if (selection != null) {
        if (selection.getField().isNullable() && selection.isNull(destIndex)) {
          dest.setNull(destIndex);
          continue;
        } else {
          sourceIndex = (int) selection.getValueAsLong(destIndex);
        }
      }
      if (input.getField().isNullable() && input.isNull(sourceIndex)) {
        dest.setNull(destIndex);
        continue;
      }
      input.getDataPointer(sourceIndex, valuePointer);
      long relativeOffset = Math.min(start, valuePointer.getLength());
      long offset = valuePointer.getOffset() + relativeOffset;
      long length = Math.min(valuePointer.getLength() - relativeOffset, this.length);
      dest.set(destIndex, (int) offset, (int) length, valuePointer.getBuf());
    }

    dest.setValueCount(destRowCount);
    return dest;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinarySlice that = (BinarySlice) o;
    return start == that.start && length == that.length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), start, length);
  }
}
