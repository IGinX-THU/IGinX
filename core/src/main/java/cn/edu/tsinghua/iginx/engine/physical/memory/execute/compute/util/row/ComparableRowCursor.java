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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ComparableRowCursor extends RowCursor implements Comparable<RowCursor> {

  private final RowCursorComparator comparator;

  public ComparableRowCursor(VectorSchemaRoot table) {
    this(table.getFieldVectors().toArray(new FieldVector[0]), 0, RowCursorComparator.of(table));
  }

  protected ComparableRowCursor(FieldVector[] columns, int index, RowCursorComparator comparator) {
    super(columns, index);
    this.comparator = comparator;
  }

  public ComparableRowCursor withPosition(int position) {
    return new ComparableRowCursor(this.columns, position, this.comparator);
  }

  @Override
  public int compareTo(RowCursor o) {
    return comparator.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    RowCursor that = (RowCursor) o;
    return comparator.equals(this, that);
  }
}
