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

import java.util.Objects;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class RowCursor extends RowPosition {

  protected final FieldVector[] columns;

  public RowCursor(VectorSchemaRoot table) {
    this(table.getFieldVectors().toArray(new FieldVector[0]), 0);
  }

  protected RowCursor(FieldVector[] columns, int index) {
    super(index);
    this.columns = columns;
  }

  public FieldVector[] getColumns() {
    return columns;
  }

  public FieldVector getColumn(int column) {
    return columns[column];
  }

  public RowCursor withPosition(int position) {
    return new RowCursor(this.columns, position);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    RowCursor that = (RowCursor) o;
    if (this.columns.length != that.columns.length) return false;
    for (int i = 0; i < this.columns.length; i++) {
      Object thisColumn = this.columns[i].getObject(this.getPosition());
      Object thatColumn = that.columns[i].getObject(that.getPosition());
      if (!Objects.deepEquals(thisColumn, thatColumn)) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    for (FieldVector column : this.columns) {
      result = 31 * result + column.hashCode(getPosition());
    }
    return result;
  }

  public void copyFromSafe(FieldVector[] sourceColumns, int sourcePosition) {
    for (int column = 0; column < this.columns.length; column++) {
      FieldVector sourceColumn = sourceColumns[column];
      FieldVector targetColumn = this.columns[column];
      targetColumn.copyFromSafe(sourcePosition, this.getPosition(), sourceColumn);
    }
  }

  public void copyFromSafe(RowCursor source) {
    copyFromSafe(source.columns, source.getPosition());
  }
}
