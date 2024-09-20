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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import java.util.Map;
import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;

class RowStreamToBatchStreamWrapper implements BatchStream {

  private final BufferAllocator allocator;
  private final RowStream rowStream;
  private final int batchSize;

  public RowStreamToBatchStreamWrapper(
      @WillNotClose BufferAllocator allocator,
      @WillCloseWhenClosed RowStream rowStream,
      int batchSize) {
    this.allocator = Objects.requireNonNull(allocator);
    this.rowStream = Objects.requireNonNull(rowStream);
    this.batchSize = batchSize;
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive rather than " + batchSize);
    }
  }

  private BatchSchema schemaCache = null;

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    if (schemaCache == null) {
      BatchSchema.Builder builder = BatchSchema.builder();

      Header header = rowStream.getHeader();
      if (header.hasKey()) {
        builder.withKey();
      }
      for (Field field : header.getFields()) {
        Map<String, String> tags = field.getTags();
        if (tags.isEmpty()) {
          builder.addField(field.getName(), field.getType());
        } else {
          builder.addField(field.getName(), field.getType(), tags);
        }
      }

      schemaCache = builder.build();
    }
    return schemaCache;
  }

  @Override
  public Batch getNext() throws PhysicalException {
    if (!rowStream.hasNext()) {
      return null;
    }

    try (Batch.Builder builder = new Batch.Builder(allocator, getSchema())) {
      builder.setInitialCapacity(batchSize);
      Header header = rowStream.getHeader();
      int rowCount = 0;
      while (rowStream.hasNext() && rowCount < batchSize) {
        Row row = rowStream.next();
        if (header.hasKey()) {
          builder.appendRow(row.getKey(), row.getValues());
        } else {
          builder.appendRow(row.getValues());
        }
        rowCount++;
      }
      builder.setRowCount(rowCount);
      return builder.build();
    }
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }
}
