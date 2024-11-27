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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;

public class MarkBuilder implements AutoCloseable {

  private BitVector bitVector;
  private int index;

  public MarkBuilder() {
    this.bitVector = null;
  }

  public MarkBuilder(BufferAllocator allocator, String name, int capacity) {
    this.bitVector = new BitVector(name, allocator);
    this.bitVector.allocateNew(capacity);
    this.index = 0;
  }

  public void appendTrue(int count) {
    if (bitVector == null) {
      return;
    }
    ConstantVectors.setOne(bitVector.getDataBuffer(), index, count);
    index += count;
  }

  public void appendFalse(int count) {
    if (bitVector == null) {
      return;
    }
    index += count;
  }

  public BitVector build(int count) {
    if (bitVector == null) {
      return null;
    }
    ConstantVectors.setAllValidity(bitVector, index);
    bitVector.setValueCount(count);
    BitVector result = bitVector;
    bitVector = null;
    return result;
  }

  @Override
  public void close() {
    if (bitVector != null) {
      bitVector.close();
    }
  }
}
