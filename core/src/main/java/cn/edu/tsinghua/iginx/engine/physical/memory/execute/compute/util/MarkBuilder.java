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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;

public class MarkBuilder implements AutoCloseable {

  private BitVector bitVector;
  private int index;
  private boolean nullable = false;

  public MarkBuilder() {
    this.bitVector = null;
  }

  public MarkBuilder(BufferAllocator allocator, String name, int capacity) {
    this.bitVector = new BitVector(name, allocator);
    this.bitVector.allocateNew(capacity);
    this.index = 0;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public void appendTrue(int count) {
    if (bitVector == null) {
      return;
    }
    bitVector.setValueCount(index + count);

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

    bitVector.setValueCount(count);
    ConstantVectors.setAllValidity(bitVector, index);
    try {
      return ValueVectors.slice(bitVector.getAllocator(), bitVector, count > index || nullable);
    } finally {
      bitVector.close();
      bitVector = null;
    }
  }

  @Override
  public void close() {
    if (bitVector != null) {
      bitVector.close();
    }
  }
}
