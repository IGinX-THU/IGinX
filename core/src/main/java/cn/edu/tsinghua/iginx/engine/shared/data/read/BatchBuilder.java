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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ColumnBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.TransferPair;

public class BatchBuilder implements AutoCloseable {
  private VectorSchemaRoot root;
  private final ColumnBuilder[] fieldBuilders;

  public BatchBuilder(@WillNotClose BufferAllocator allocator, BatchSchema schema, int rowCount) {
    this(allocator, schema);
    for (FieldVector vector : root.getFieldVectors()) {
      vector.setInitialCapacity(rowCount);
      if (vector instanceof FixedWidthVector) {
        ((FixedWidthVector) vector).allocateNew(rowCount);
      } else if (vector instanceof VariableWidthFieldVector) {
        ((VariableWidthVector) vector).allocateNew(rowCount);
      }
    }
  }

  public BatchBuilder(@WillNotClose BufferAllocator allocator, BatchSchema schema) {
    this.root = VectorSchemaRoot.create(schema.raw(), allocator);
    this.fieldBuilders =
        root.getFieldVectors().stream().map(ColumnBuilder::create).toArray(ColumnBuilder[]::new);
  }

  @Override
  public void close() {
    if (root != null) {
      root.close();
      root = null;
    }
  }

  public BatchBuilder append(long key, Object... values) {
    if (values.length + 1 != fieldBuilders.length) {
      throw new IllegalArgumentException(
          "Expected key and "
              + fieldBuilders.length
              + " values, but got "
              + values.length
              + " values");
    }
    fieldBuilders[0].append(key);
    for (int i = 0; i < values.length; i++) {
      fieldBuilders[i + 1].append(values[i]);
    }
    return this;
  }

  public BatchBuilder append(Object... values) {
    if (values.length != fieldBuilders.length) {
      throw new IllegalArgumentException(
          "Expected " + fieldBuilders.length + " values, but got " + values.length);
    }
    for (int i = 0; i < values.length; i++) {
      fieldBuilders[i].append(values[i]);
    }
    return this;
  }

  public Batch build(int rowCount) {
    root.setRowCount(rowCount);
    Batch batch = Batch.of(root);
    root = null;
    return batch;
  }

  public ColumnBuilder getField(int index) {
    return fieldBuilders[index];
  }

  public TransferPair getTransferPair(int targetIndex, ValueVector source) {
    return source.makeTransferPair(root.getFieldVectors().get(targetIndex));
  }
}
