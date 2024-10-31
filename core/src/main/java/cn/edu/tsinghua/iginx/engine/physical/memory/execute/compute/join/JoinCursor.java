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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

class JoinCursor {

  private final FieldVector[] keyColumns;
  private final FieldVector[] valueColumns;
  private final int index;
  private boolean matched = false;

  public JoinCursor(VectorSchemaRoot keys, VectorSchemaRoot values) {
    this(
        keys.getFieldVectors().toArray(new FieldVector[0]),
        values.getFieldVectors().toArray(new FieldVector[0]),
        0);
  }

  protected JoinCursor(FieldVector[] keyColumns, FieldVector[] valueColumns, int index) {
    this.keyColumns = keyColumns;
    this.valueColumns = valueColumns;
    this.index = index;
  }

  public JoinCursor withPosition(int position) {
    return new JoinCursor(this.keyColumns, this.valueColumns, position);
  }

  public void copyKeysTo(RowCursor cursor) {
    cursor.copyFrom(keyColumns, index);
  }

  public void copyValuesTo(RowCursor cursor) {
    matched = true;
    cursor.copyFrom(valueColumns, index);
  }

  public boolean isUnmatched() {
    return !matched;
  }
}
