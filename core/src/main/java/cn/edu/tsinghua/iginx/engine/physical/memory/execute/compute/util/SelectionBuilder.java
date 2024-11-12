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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class SelectionBuilder implements AutoCloseable {

  private IntVector selection;
  private int index = 0;

  public SelectionBuilder(BufferAllocator allocator, String name, int initCapacity) {
    selection = new IntVector(new Field(name, FieldType.notNullable(Types.MinorType.INT.getType()), null), allocator);
    selection.allocateNew(initCapacity);
  }

  @Override
  public void close() {
    if (selection != null) {
      selection.close();
    }
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
    ConstantVectors.setAllValidity(selection, index);
    selection.setValueCount(count);
    IntVector selection = this.selection;
    this.selection = null;
    return selection;
  }
}
