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
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.local.ParquetFileReader;
import org.apache.parquet.local.ParquetReadOptions;
import org.apache.parquet.local.ParquetRecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IParquetReader implements AutoCloseable {
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

  public MessageType getSchema() {
    return schema;
  }

  public Map<String, String> getExtra() {
    return Collections.unmodifiableMap(metadata.getFileMetaData().getKeyValueMetaData());
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
  public void close() throws Exception {
    if (internalReader != null) {
      internalReader.close();
    }
    LOGGER.info("read {} records", count);
  }

  public long getRowCount() {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

  public static class Builder {

    private final ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
    private final InputFile localInputfile;
    private boolean skip = false;
    private Set<String> fields;

    public Builder(LocalInputFile localInputFile) {
      this.localInputfile = localInputFile;
    }

    public IParquetReader build() throws IOException {
      ParquetReadOptions options = optionsBuilder.build();
      ParquetMetadata footer;
      try (SeekableInputStream in = localInputfile.newStream()) {
        footer = ParquetFileReader.readFooter(localInputfile, options, in);
      }

      MessageType schema = footer.getFileMetaData().getSchema();
      MessageType requestedSchema;
      if (fields == null) {
        requestedSchema = schema;
      } else {
        requestedSchema = ProjectUtils.projectMessageType(schema, fields);
        LOGGER.info("project schema with {} as {}", fields, requestedSchema);
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

    public Builder project(Set<String> fields) {
      this.fields = Objects.requireNonNull(fields);
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
  }
}
