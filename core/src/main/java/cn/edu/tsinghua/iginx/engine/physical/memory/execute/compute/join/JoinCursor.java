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
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

final class JoinCursor {

  private interface Copier {
    void copyTo(ValueVector target, int targetIndex, int thisIndex);
  }

  private final Copier[] onFieldColumnCopiers;
  private final Copier[] outputFieldColumnCopiers;
  private final int index;
  private boolean matched = false;

  public JoinCursor(VectorSchemaRoot onField, VectorSchemaRoot outputFields) {
    this(
        onField.getFieldVectors().stream().map(JoinCursor::toCopier).toArray(Copier[]::new),
        outputFields.getFieldVectors().stream().map(JoinCursor::toCopier).toArray(Copier[]::new),
        0);
  }

  JoinCursor(Copier[] onFieldColumns, Copier[] outputFieldColumns, int index) {
    this.onFieldColumnCopiers = onFieldColumns;
    this.outputFieldColumnCopiers = outputFieldColumns;
    this.index = index;
  }

  private static Copier toCopier(FieldVector column) {
    if (column instanceof FixedWidthVector) {
      return (target, targetIndex, thisIndex) -> {
        target.copyFrom(thisIndex, targetIndex, column);
      };
    } else {
      return (target, targetIndex, thisIndex) -> {
        target.copyFromSafe(thisIndex, targetIndex, column);
      };
    }
  }

  public JoinCursor withPosition(int position) {
    return new JoinCursor(onFieldColumnCopiers, outputFieldColumnCopiers, position);
  }

  public void appendOnFieldsTo(RowCursor cursor) {
    copyTo(cursor.getColumns(), cursor.getPosition(), onFieldColumnCopiers, index);
  }

  public void appendOutputFieldsTo(RowCursor cursor) {
    copyTo(cursor.getColumns(), cursor.getPosition(), outputFieldColumnCopiers, index);
  }

  private static void copyTo(
      FieldVector[] targets, int targetIndex, Copier[] sources, int sourceIndex) {
    for (int i = 0; i < targets.length; i++) {
      sources[i].copyTo(targets[i], targetIndex, sourceIndex);
    }
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
