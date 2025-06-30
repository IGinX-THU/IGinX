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
import org.apache.arrow.vector.IntVector;

public class SelectionBuilder implements AutoCloseable {

  private final BufferAllocator allocator;
  private IntVector selection;
  private int index = 0;
  private boolean nullable = false;

  public SelectionBuilder(BufferAllocator allocator, String name, int initCapacity) {
    this.allocator = allocator;
    selection = new IntVector(name, allocator);
    selection.allocateNew(initCapacity);
  }

  @Override
  public void close() {
    if (selection != null) {
      selection.close();
    }
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public void append(int integer) {
    if (index >= selection.getValueCapacity()) {
      selection.reAlloc();
    }
    selection.getDataBuffer().setInt(index * (long) Integer.BYTES, integer);
    index++;
  }

  public IntVector build() {
    return build(index);
  }

  public IntVector build(int count) {
    selection.setValueCount(count);
    ConstantVectors.setAllValidity(selection, index);
    try {
      return ValueVectors.slice(allocator, selection, count > index || nullable);
    } finally {
      selection.close();
      selection = null;
    }
  }
}
