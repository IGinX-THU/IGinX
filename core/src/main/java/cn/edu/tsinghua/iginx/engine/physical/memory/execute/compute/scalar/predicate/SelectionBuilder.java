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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate;

import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

public class SelectionBuilder implements AutoCloseable {

  private IntVector selection;
  private final ArrowBuf dataBuffer;
  private int index = 0;

  public SelectionBuilder(BufferAllocator allocator, String name, int capacity) {
    selection =
        new IntVector(name, FieldType.notNullable(Types.MinorType.INT.getType()), allocator);
    selection.allocateNew(capacity);
    dataBuffer = selection.getDataBuffer();
  }

  @Override
  public void close() {
    if (selection != null) {
      selection.close();
    }
  }

  public void append(int integer) {
    dataBuffer.setInt(index * (long) Integer.BYTES, integer);
    index++;
  }

  public BaseIntVector build() {
    ConstantVectors.setValueCountWithValidity(selection, index);
    IntVector selection = this.selection;
    this.selection = null;
    return selection;
  }
}
