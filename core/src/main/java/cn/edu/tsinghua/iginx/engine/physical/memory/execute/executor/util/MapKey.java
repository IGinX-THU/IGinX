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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ColumnBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public final class MapKey {
  private final Object[] key;
  private final transient int hashCode;

  public MapKey(VectorSchemaRoot data, int index) {
    this(VectorSchemaRoots.get(data, index));
  }

  MapKey(Object[] key) {
    this.key = key;
    hashCode = Arrays.deepHashCode(key);
  }

  public static VectorSchemaRoot merge(
      BufferAllocator allocator, Schema groupKeySchema, List<Object[]> groupKeys)
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
    for (Object[] groupKey : groupKeys) {
      for (int i = 0; i < groupKey.length; i++) {
        columnBuilders.get(i).append(groupKey[i]);
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
    if (!(o instanceof MapKey)) {
      return false;
    }
    MapKey mapKey = (MapKey) o;
    return Arrays.deepEquals(key, mapKey.key);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
