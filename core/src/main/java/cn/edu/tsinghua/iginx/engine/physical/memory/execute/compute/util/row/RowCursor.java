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

import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class RowCursor extends RowIndex {

  private final FieldVector[] columns;

  public RowCursor(@WillNotClose VectorSchemaRoot table) {
    this(table.getFieldVectors().toArray(new FieldVector[0]), 0);
  }

  protected RowCursor(FieldVector[] columns, int index) {
    super(0);
    this.columns = columns;
  }

  public FieldVector getColumn(int columnIndex) {
    return columns[columnIndex];
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    for (FieldVector column : columns) {
      result = 31 * result + column.hashCode(index);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RowCursor)) {
      return false;
    }
    RowCursor other = (RowCursor) obj;
    if (columns.length != other.columns.length) {
      return false;
    }
    for (int i = 0; i < columns.length; i++) {
      Object left = columns[i].getObject(index);
      Object right = other.columns[i].getObject(index);
      if (!Objects.equals(left, right)) {
        return false;
      }
    }
    return true;
  }
}
