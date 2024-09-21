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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class RowStreamToBatchStreamWrapper implements BatchStream {

  private final ExecutorContext context;
  private final RowStream rowStream;

  public RowStreamToBatchStreamWrapper(ExecutorContext context, RowStream rowStream) {
    this.context = Objects.requireNonNull(context);
    this.rowStream = Objects.requireNonNull(rowStream);
  }

  private BatchSchema schemaCache = null;

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    if (schemaCache == null) {
      Header header;
      try (StopWatch watch = new StopWatch(context::addFetchTime)) {
        header = rowStream.getHeader();
      }
      try (StopWatch watch = new StopWatch(context::addPipelineComputeTime)) {
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
    }

    return schemaCache;
  }

  @Override
  public Batch getNext() throws PhysicalException {
    List<Row> rows = new ArrayList<>();
    try (StopWatch watch = new StopWatch(context::addFetchTime)) {
      while (rowStream.hasNext() && rows.size() < context.getMaxBatchRowCount()) {
        rows.add(rowStream.next());
      }
    }

    if (rows.isEmpty()) {
      return null;
    }

    context.addProducedRowNumber(rows.size());

    boolean hasKey = getSchema().hasKey();
    try (StopWatch watch = new StopWatch(context::addPipelineComputeTime)) {
      try (Batch.Builder builder =
          new Batch.Builder(context.getAllocator(), getSchema(), rows.size())) {
        for (Row row : rows) {
          if (hasKey) {
            builder.append(row.getKey(), row.getValues());
          } else {
            builder.append(row.getValues());
          }
        }
        return builder.build(rows.size());
      }
    }
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }
}
