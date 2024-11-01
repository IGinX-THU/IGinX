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
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinOutputBuffer {
  private final int batchSize;
  private final JoinOption joinOption;
  private final Queue<VectorSchemaRoot> staged;
  private final VectorSchemaRoot buffer;
  private final RowCursor leftOutputCursor;
  private final RowCursor rightOutputCursor;
  private final Optional<BitVector> markColumn;

  JoinOutputBuffer(
      BufferAllocator allocator,
      int batchSize,
      JoinOption joinOption,
      Schema leftOutputSchema,
      Schema rightOutputSchema) {
    this.batchSize = batchSize;
    this.joinOption = joinOption;

    VectorSchemaRoot leftBuffer = VectorSchemaRoot.create(leftOutputSchema, allocator);
    VectorSchemaRoot rightBuffer = VectorSchemaRoot.create(rightOutputSchema, allocator);
    this.leftOutputCursor = new RowCursor(leftBuffer);
    this.rightOutputCursor = new RowCursor(rightBuffer);

    List<FieldVector> allBuffers = new ArrayList<>();
    allBuffers.addAll(leftBuffer.getFieldVectors());
    allBuffers.addAll(rightBuffer.getFieldVectors());

    if (joinOption.needMark()) {
      BitVector markColumn = new BitVector(joinOption.markColumnName(), allocator);
      allBuffers.add(markColumn);
      this.markColumn = Optional.of(markColumn);
    } else {
      this.markColumn = Optional.empty();
    }

    this.buffer = new VectorSchemaRoot(allBuffers);
    resetBuffer();

    this.staged = new ArrayDeque<>();
  }

  public void flush() {
    if (getBufferedRowCount() > 0) {
      buffer.setRowCount(getBufferedRowCount());
      staged.add(buffer.slice(0));
      resetBuffer();
    }
  }

  public boolean isEmpty() {
    return staged.isEmpty();
  }

  public VectorSchemaRoot poll() {
    return staged.remove();
  }

  void append(Iterable<JoinCursor> buildSideCursors, Iterable<JoinCursor> probeSideCursors) {
    Iterator<JoinCursor> buildSideIterator = buildSideCursors.iterator();
    Iterator<JoinCursor> probeSideIterator = probeSideCursors.iterator();

    while (buildSideIterator.hasNext() || probeSideIterator.hasNext()) {
      if (buildSideIterator.hasNext()) {
        JoinCursor buildCursor = buildSideIterator.next();
        buildCursor.copyValuesTo(leftOutputCursor);
        leftOutputCursor.setPosition(leftOutputCursor.getPosition() + 1);
      }
      if (probeSideIterator.hasNext()) {
        JoinCursor probeCursor = probeSideIterator.next();
        probeCursor.copyValuesTo(rightOutputCursor);
        markColumn.ifPresent(
            marks -> {
              boolean mark =
                  joinOption.isAntiMark() ? probeCursor.isUnmatched() : probeCursor.isMatched();
              marks.set(rightOutputCursor.getPosition(), mark ? 1 : 0);
            });
        rightOutputCursor.setPosition(rightOutputCursor.getPosition() + 1);
      }
      if (getBufferedRowCount() >= batchSize) {
        flush();
      }
    }
  }

  private void resetBuffer() {
    for (FieldVector vector : buffer.getFieldVectors()) {
      vector.clear();
      vector.setInitialCapacity(batchSize);
    }
    buffer.setRowCount(batchSize);
    leftOutputCursor.setPosition(0);
    rightOutputCursor.setPosition(0);
  }

  private int getBufferedRowCount() {
    return Math.max(leftOutputCursor.getPosition(), rightOutputCursor.getPosition());
  }

  void close() {
    staged.forEach(VectorSchemaRoot::close);
    buffer.close();
  }
}
