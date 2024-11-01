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

final class JoinCursor {

  private final FieldVector[] onFieldColumns;
  private final FieldVector[] outputFieldColumns;
  private final int index;
  private boolean matched = false;

  public JoinCursor(VectorSchemaRoot onField, VectorSchemaRoot outputFields) {
    this(
        onField.getFieldVectors().toArray(new FieldVector[0]),
        outputFields.getFieldVectors().toArray(new FieldVector[0]),
        0);
  }

  JoinCursor(FieldVector[] onFieldColumns, FieldVector[] outputFieldColumns, int index) {
    this.onFieldColumns = onFieldColumns;
    this.outputFieldColumns = outputFieldColumns;
    this.index = index;
  }

  public JoinCursor withPosition(int position) {
    return new JoinCursor(this.onFieldColumns, this.outputFieldColumns, position);
  }

  public void copyKeysTo(RowCursor cursor) {
    cursor.copyFrom(onFieldColumns, index);
  }

  public void copyValuesTo(RowCursor cursor) {
    cursor.copyFrom(outputFieldColumns, index);
  }

  public void markMatched() {
    matched = true;
  }

  public boolean isUnmatched() {
    return !matched;
  }

  public boolean isMatched() {
    return !isUnmatched();
  }
}
