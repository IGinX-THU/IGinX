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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row;

import javax.annotation.WillNotClose;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ComparableRowCursor extends RowCursor implements Comparable<RowCursor> {

  private final RowCursorComparator comparator;

  public ComparableRowCursor(@WillNotClose VectorSchemaRoot table) {
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
