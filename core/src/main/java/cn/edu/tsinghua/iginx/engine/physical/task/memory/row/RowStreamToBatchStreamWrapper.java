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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.row;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskMetrics;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import com.google.common.base.Strings;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class RowStreamToBatchStreamWrapper implements BatchStream {

  private final BufferAllocator allocator;
  private final RowStream rowStream;
  private final TaskMetrics metrics;
  private final int batchRowCount;
  private long sequenceNumber = 0;

  public RowStreamToBatchStreamWrapper(
      BufferAllocator allocator, RowStream rowStream, int batchRowCount) {
    this(allocator, rowStream, TaskMetrics.NO_OP, batchRowCount);
  }

  public RowStreamToBatchStreamWrapper(
      BufferAllocator allocator, RowStream rowStream, TaskMetrics metrics, int batchRowCount) {
    Preconditions.checkArgument(batchRowCount > 0);
    this.allocator = Objects.requireNonNull(allocator);
    this.rowStream = Objects.requireNonNull(rowStream);
    this.metrics = Objects.requireNonNull(metrics);
    this.batchRowCount = batchRowCount;
  }

  private BatchSchema schemaCache = null;

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    if (schemaCache == null) {
      Header header = rowStream.getHeader();
      try (StopWatch watch = new StopWatch(metrics::accumulateCpuTime)) {
        schemaCache = getSchema(header);
      }
    }

    return schemaCache;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return rowStream.hasNext();
  }

  public static BatchSchema getSchema(Header header) {
    BatchSchema.Builder builder = BatchSchema.builder();
    if (header.hasKey()) {
      builder.withKey();
    }
    for (Field field : header.getFields()) {
      builder.addField(toArrowField(field));
    }
    return builder.build();
  }

  public static org.apache.arrow.vector.types.pojo.Field toArrowField(Field field) {
    String fieldName = field.getName();
    ArrowType arrowType = Schemas.toArrowType(field.getType());
    Map<String, String> metadata = new HashMap<>(field.getTags());
    String fullFieldName = field.getFullName();
    if (metadata.isEmpty() && !Objects.equals(fieldName, fullFieldName)) {
      int commonPrefixLength = Strings.commonPrefix(fieldName, fullFieldName).length();
      String suffix = fullFieldName.substring(commonPrefixLength);
      StringBuilder sb = new StringBuilder();
      TagKVUtils.fillNameAndTagMap(suffix, sb, metadata);
    }
    return new org.apache.arrow.vector.types.pojo.Field(
        fieldName, new FieldType(true, arrowType, null, metadata), Collections.emptyList());
  }

  @Override
  public Batch getNext() throws PhysicalException {
    BatchSchema schema = getSchema();
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < batchRowCount && rowStream.hasNext(); i++) {
      rows.add(rowStream.next());
    }

    if (rows.isEmpty()) {
      throw new NoSuchElementException();
    }

    boolean hasKey = schema.hasKey();
    try (StopWatch ignored = new StopWatch(metrics::accumulateCpuTime)) {
      try (BatchBuilder builder = new BatchBuilder(allocator, schema, rows.size())) {
        for (Row row : rows) {
          if (hasKey) {
            builder.append(row.getKey(), row.getValues());
          } else {
            builder.append(row.getValues());
          }
        }
        metrics.accumulateAffectRows(rows.size());
        Batch batch = builder.build(rows.size());
        batch.setSequenceNumber(sequenceNumber++);
        return batch;
      }
    }
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }
}
