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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import org.apache.arrow.memory.BufferAllocator;

class RowStreamToBatchStreamWrapper implements BatchStream {

  private final BufferAllocator allocator;
  private final RowStream rowStream;
  private final int batchRowCount;

  public RowStreamToBatchStreamWrapper(
      BufferAllocator allocator, RowStream rowStream, int batchRowCount) {
    this.allocator = Objects.requireNonNull(allocator);
    this.rowStream = Objects.requireNonNull(rowStream);
    this.batchRowCount = batchRowCount;
  }

  private BatchSchema schemaCache = null;

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    if (schemaCache == null) {
      Header header = rowStream.getHeader();
      BatchSchema.Builder builder = BatchSchema.builder();
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
    boolean hasKey = getSchema().hasKey();

    try (Batch.Builder builder = new Batch.Builder(allocator, getSchema(), batchRowCount)) {
      int count;
      for (count = 0; rowStream.hasNext() && count < batchRowCount; count++) {
        Row row = rowStream.next();
        if (hasKey) {
          builder.append(row.getKey(), row.getValues());
        } else {
          builder.append(row.getValues());
        }
      }
      return builder.build(count);
    }
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }
}
