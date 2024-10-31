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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class JoinOutputBuffer {
  private final BufferAllocator allocator;
  private final int batchSize;
  private final Queue<VectorSchemaRoot> staged;
  private final VectorSchemaRoot buffer;
  private final RowCursor leftOutputCursor;
  private final RowCursor rightOutputCursor;
  private int bufferedRowCount = 0;

  JoinOutputBuffer(
      BufferAllocator allocator, int batchSize, Schema leftOutputSchema, Schema rightOutputSchema) {
    this.allocator = allocator;
    this.batchSize = batchSize;

    VectorSchemaRoot leftBuffer = VectorSchemaRoot.create(leftOutputSchema, allocator);
    VectorSchemaRoot rightBuffer = VectorSchemaRoot.create(rightOutputSchema, allocator);
    this.leftOutputCursor = new RowCursor(leftBuffer);
    this.rightOutputCursor = new RowCursor(rightBuffer);

    List<FieldVector> allBuffers = new ArrayList<>();
    allBuffers.addAll(leftBuffer.getFieldVectors());
    allBuffers.addAll(rightBuffer.getFieldVectors());
    this.buffer = new VectorSchemaRoot(allBuffers);
    resetBuffer();

    this.staged = new ArrayDeque<>();
  }

  public void flush() {
    buffer.setRowCount(bufferedRowCount);
    staged.add(buffer.slice(0));
    resetBuffer();
  }

  public boolean isEmpty() {
    return staged.isEmpty();
  }

  public VectorSchemaRoot poll() {
    return staged.remove();
  }

  void append(Iterable<JoinCursor> leftCursors, Iterable<JoinCursor> rightCursors) {
    Iterator<JoinCursor> leftIterator = leftCursors.iterator();
    Iterator<JoinCursor> rightIterator = rightCursors.iterator();

    while (leftIterator.hasNext() || rightIterator.hasNext()) {
      if (leftIterator.hasNext()) {
        leftIterator.next().copyValuesTo(leftOutputCursor);
      }
      if (rightIterator.hasNext()) {
        rightIterator.next().copyValuesTo(rightOutputCursor);
      }
      bufferedRowCount++;
      leftOutputCursor.setPosition(bufferedRowCount);
      rightOutputCursor.setPosition(bufferedRowCount);
      if (bufferedRowCount >= batchSize) {
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
    bufferedRowCount = 0;
  }

  void close() {
    staged.forEach(VectorSchemaRoot::close);
    buffer.close();
  }
}
