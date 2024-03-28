/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.format.parquet.ParquetReader;
import cn.edu.tsinghua.iginx.format.parquet.codec.DefaultCodecFactory;
import cn.edu.tsinghua.iginx.format.parquet.io.LocalInputFile;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.bytes.ByteBufferAllocator;
import shaded.iginx.org.apache.parquet.column.statistics.LongStatistics;
import shaded.iginx.org.apache.parquet.filter2.compat.FilterCompat;
import shaded.iginx.org.apache.parquet.filter2.predicate.FilterPredicate;
import shaded.iginx.org.apache.parquet.hadoop.ExportedParquetRecordReader;
import shaded.iginx.org.apache.parquet.hadoop.metadata.BlockMetaData;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.InputFile;
import shaded.iginx.org.apache.parquet.io.api.RecordMaterializer;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class IParquetReader extends ParquetReader<IRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IParquetReader.class);

  private final MessageType schema;
  private final ParquetMetadata metadata;

  private IParquetReader(
      ExportedParquetRecordReader<IRecord> internalReader,
      MessageType schema,
      ParquetMetadata metadata) {
    super(internalReader);
    this.schema = schema;
    this.metadata = metadata;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public MessageType getSchema() {
    return schema;
  }

  public long getRowCount() {
    return getRowCount(metadata);
  }

  public static long getRowCount(ParquetMetadata metadata) {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

  public Range<Long> getRange() {
    return getRange(metadata);
  }

  public static Range<Long> getRange(ParquetMetadata metadata) {
    MessageType schema = metadata.getFileMetaData().getSchema();
    if (schema.containsPath(new String[] {Constants.KEY_FIELD_NAME})) {
      Type type = schema.getType(Constants.KEY_FIELD_NAME);
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
          return getKeyRange(metadata);
        }
      }
    }
    return Range.closedOpen(0L, getRowCount(metadata));
  }

  private static Range<Long> getKeyRange(ParquetMetadata metadata) {
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
        throw new IllegalArgumentException(
            "Unsupported data type: " + primitiveType.getPrimitiveTypeName());
    }
  }

  public static class Builder extends ParquetReader.Builder<IRecord, IParquetReader, Builder> {

    private final InputFile localInputfile;
    private Set<String> fields;
    private MessageType requestedSchema;

    public Builder(LocalInputFile localInputFile) {
      this.localInputfile = localInputFile;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected RecordMaterializer<IRecord> materializer(
        MessageType messageType, Map<String, String> map) {
      this.requestedSchema = messageType;
      return new IRecordMaterializer(messageType);
    }

    @Override
    public IParquetReader build() throws IOException {
      ParquetMetadata footer = readFooter(localInputfile);
      return build(footer);
    }

    public IParquetReader build(ParquetMetadata footer) throws IOException {
      ExportedParquetRecordReader<IRecord> internalReader = build(localInputfile, footer);
      return new IParquetReader(internalReader, requestedSchema, footer);
    }

    public Builder project(Set<String> fields) {
      return super.withSchemaConverter(schema -> ProjectUtils.projectMessageType(schema, fields));
    }

    public Builder filter(Filter filter) {
      Pair<FilterPredicate, Boolean> filterPredicate =
          FilterUtils.toFilterPredicate(Objects.requireNonNull(filter));
      if (filterPredicate.k != null) {
        return super.withFilter(FilterCompat.get(filterPredicate.k));
      } else {
        if (!filterPredicate.v) {
          throw new StorageRuntimeException("Filter is False. No data will be read.");
        }
      }
      return this;
    }

    public Builder withCodecFactory(ByteBufferAllocator byteBufferAllocator, int lz4BufferSize) {
      super.withCodecFactory(
          new DefaultCodecFactory(
              byteBufferAllocator,
              lz4BufferSize,
              DefaultCodecFactory.DEFAULT_ZSTD_LEVEL,
              DefaultCodecFactory.DEFAULT_ZSTD_WORKERS));
      return this;
    }
  }
}
