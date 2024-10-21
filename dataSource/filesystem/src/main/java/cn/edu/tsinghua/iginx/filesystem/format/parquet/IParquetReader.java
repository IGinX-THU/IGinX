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
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.collect.Range;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.ParquetReadOptions;
import shaded.iginx.org.apache.parquet.column.statistics.LongStatistics;
import shaded.iginx.org.apache.parquet.filter2.compat.FilterCompat;
import shaded.iginx.org.apache.parquet.filter2.predicate.FilterPredicate;
import shaded.iginx.org.apache.parquet.hadoop.CodecFactory;
import shaded.iginx.org.apache.parquet.hadoop.ParquetFileReader;
import shaded.iginx.org.apache.parquet.hadoop.ParquetRecordReader;
import shaded.iginx.org.apache.parquet.hadoop.metadata.BlockMetaData;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnPath;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.InputFile;
import shaded.iginx.org.apache.parquet.io.LocalInputFile;
import shaded.iginx.org.apache.parquet.io.SeekableInputStream;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class IParquetReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IParquetReader.class);

  private final ParquetRecordReader<IRecord> internalReader;
  private final MessageType schema;
  private final ParquetMetadata metadata;

  private long count = 0;

  private IParquetReader(
      ParquetRecordReader<IRecord> internalReader, MessageType schema, ParquetMetadata metadata) {
    this.internalReader = internalReader;
    this.schema = schema;
    this.metadata = metadata;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public static Map<ColumnPath, Long> getCountsOf(ParquetMetadata meta) {
    Map<ColumnPath, Long> counts = new HashMap<>();
    for (BlockMetaData block : meta.getBlocks()) {
      List<ColumnChunkMetaData> columns = block.getColumns();
      for (ColumnChunkMetaData column : columns) {
        ColumnPath path = column.getPath();
        long count = column.getValueCount();
        long nulls = column.getStatistics().getNumNulls();
        long nonNulls = count - nulls;
        counts.compute(path, (k, v) -> v == null ? nonNulls : v + nonNulls);
      }
    }
    return counts;
  }

  public static Map<String, String> encodeCounts(Map<String, Long> counts) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toString());
    }
    return result;
  }

  public MessageType getSchema() {
    return schema;
  }

  public IRecord read() throws IOException {
    if (internalReader == null) {
      return null;
    }
    if (!internalReader.nextKeyValue()) {
      return null;
    }
    count++;
    return internalReader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    if (internalReader != null) {
      internalReader.close();
    }
    LOGGER.debug("read {} records", count);
  }

  public long getRowCount() {
    return getRowCountOf(metadata);
  }

  public static long getRowCountOf(ParquetMetadata metadata) {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

  public Range<Long> getRange() {
    return getRangeOf(metadata);
  }

  public static Range<Long> getRangeOf(ParquetMetadata metadata) {
    MessageType schema = metadata.getFileMetaData().getSchema();
    if (schema.containsPath(new String[] {Constants.KEY_FIELD_NAME})) {
      Type type = schema.getType(Constants.KEY_FIELD_NAME);
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
          return getKeyRangeOf(metadata);
        }
      }
    }
    return Range.closedOpen(0L, getRowCountOf(metadata));
  }

  private static Range<Long> getKeyRangeOf(ParquetMetadata metadata) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;

    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getPath().toDotString().equals(Constants.KEY_FIELD_NAME)) {
          LongStatistics statistics = (LongStatistics) column.getStatistics();
          min = Math.min(min, statistics.genericGetMin());
          max = Math.max(max, statistics.genericGetMax());
        }
      }
    }

    if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
      return Range.closedOpen(0L, 0L);
    }

    return Range.closed(min, max);
  }

  public ParquetMetadata getMeta() {
    return metadata;
  }

  public static DataType toIginxType(PrimitiveType primitiveType) {
    if (primitiveType.getRepetition().equals(PrimitiveType.Repetition.REPEATED)) {
      return DataType.BINARY;
    }
    switch (primitiveType.getPrimitiveTypeName()) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
        return DataType.BINARY;
      default:
        throw new RuntimeException(
            "Unsupported data type: " + primitiveType.getPrimitiveTypeName());
    }
  }

  public long getCurrentRowIndex() {
    return internalReader.getCurrentRowIndex();
  }

  public static class Builder {

    private final ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
    private final InputFile localInputfile;
    private boolean skip = false;
    private Set<String> fields;
    private boolean hasKey;

    public Builder(LocalInputFile localInputFile) {
      this.localInputfile = localInputFile;
    }

    public IParquetReader build() throws IOException {
      ParquetReadOptions options = optionsBuilder.build();
      ParquetMetadata footer;
      try (SeekableInputStream in = localInputfile.newStream()) {
        footer = ParquetFileReader.readFooter(localInputfile, options, in);
      }

      return build(footer, options);
    }

    public IParquetReader build(ParquetMetadata footer) throws IOException {
      return build(footer, optionsBuilder.build());
    }

    private IParquetReader build(ParquetMetadata footer, ParquetReadOptions options)
        throws IOException {
      MessageType schema = footer.getFileMetaData().getSchema();
      MessageType requestedSchema;
      if (fields == null) {
        requestedSchema = schema;
      } else {
        requestedSchema = ProjectUtils.projectMessageType(schema, fields, hasKey);
        LOGGER.debug("project schema with {} as {}", fields, requestedSchema);
      }

      if (skip) {
        return new IParquetReader(null, requestedSchema, footer);
      }

      ParquetFileReader reader = new ParquetFileReader(localInputfile, footer, options);
      reader.setRequestedSchema(requestedSchema);
      ParquetRecordReader<IRecord> internalReader =
          new ParquetRecordReader<>(new IRecordMaterializer(requestedSchema), reader, options);
      return new IParquetReader(internalReader, requestedSchema, footer);
    }

    @Override
    public String toString() {
      return "Builder{"
          + "optionsBuilder="
          + optionsBuilder
          + ", localInputfile="
          + localInputfile
          + '}';
    }

    public Builder project(Set<String> fields) {
      return project(fields, true);
    }

    public Builder project(Set<String> fields, boolean hasKey) {
      this.fields = Objects.requireNonNull(fields);
      this.hasKey = hasKey;
      return this;
    }

    public Builder filter(Filter filter) {
      Pair<FilterPredicate, Boolean> filterPredicate =
          FilterUtils.toFilterPredicate(Objects.requireNonNull(filter));
      if (filterPredicate.k != null) {
        optionsBuilder.withRecordFilter(FilterCompat.get(filterPredicate.k));
      } else {
        skip = !filterPredicate.v;
      }
      return this;
    }

    public Builder withCodecFactory(int lz4BufferSize) {
      optionsBuilder.withCodecFactory(
          new CodecFactory(
              lz4BufferSize, CodecFactory.DEFAULT_ZSTD_LEVEL, CodecFactory.DEFAULT_ZSTD_WORKERS));
      return this;
    }
  }
}
