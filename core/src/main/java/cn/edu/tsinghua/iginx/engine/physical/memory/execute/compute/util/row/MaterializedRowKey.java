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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ColumnBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public final class MaterializedRowKey {
  private final Object[] key;
  private final transient int hashCode;

  public MaterializedRowKey(Object[] key) {
    this.key = key;
    hashCode = Arrays.deepHashCode(key);
  }

  public static MaterializedRowKey of(RowCursor cursor) {
    Object[] key = ValueVectors.getObjects(cursor.getColumns(), cursor.getPosition());
    return new MaterializedRowKey(key);
  }

  public static VectorSchemaRoot merge(
      BufferAllocator allocator, Schema groupKeySchema, List<MaterializedRowKey> groupKeys)
      throws ComputeException {
    VectorSchemaRoot root = VectorSchemaRoot.create(groupKeySchema, allocator);
    List<ColumnBuilder> columnBuilders = new ArrayList<>();
    try {
      for (FieldVector fieldVector : root.getFieldVectors()) {
        if (!ColumnBuilder.support(fieldVector)) {
          throw new ComputeException("Unsupported type: " + fieldVector.getMinorType());
        }
        columnBuilders.add(ColumnBuilder.create(fieldVector));
        fieldVector.setInitialCapacity(groupKeys.size());
      }
    } catch (ComputeException e) {
      root.close();
      throw e;
    }
    for (FieldVector fieldVector : root.getFieldVectors()) {
      if (fieldVector instanceof FixedWidthVector) {
        ((FixedWidthVector) fieldVector).allocateNew(groupKeys.size());
      }
    }

    for (MaterializedRowKey groupKey : groupKeys) {
      for (int i = 0; i < groupKey.key.length; i++) {
        columnBuilders.get(i).append(groupKey.key[i]);
      }
    }
    root.setRowCount(groupKeys.size());
    return root;
  }

  public Object[] getData() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MaterializedRowKey)) {
      return false;
    }
    MaterializedRowKey materializedRowKey = (MaterializedRowKey) o;
    return Arrays.deepEquals(key, materializedRowKey.key);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
