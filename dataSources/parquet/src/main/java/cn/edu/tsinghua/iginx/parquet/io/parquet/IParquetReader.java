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
import java.util.Set;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.local.ParquetFileReader;
import org.apache.parquet.local.ParquetReadOptions;
import org.apache.parquet.local.ParquetRecordReader;
import org.apache.parquet.schema.MessageType;

public class IParquetReader implements AutoCloseable {

  private final ParquetRecordReader<IRecord> internalReader;
  private final MessageType schema;

  private final Map<String, String> extra;

  private IParquetReader(
      ParquetRecordReader<IRecord> internalReader, MessageType schema, Map<String, String> extra) {
    this.internalReader = internalReader;
    this.schema = schema;
    this.extra = extra;
  }

  private IParquetReader(MessageType schema, Map<String, String> extra) {
    this.extra = extra;
    this.internalReader = null;
    this.schema = schema;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public MessageType getSchema() {
    return schema;
  }

  public String getExtraMetaData(String key) {
    return extra.get(key);
  }

  public Map<String, String> getExtra() {
    return Collections.unmodifiableMap(extra);
  }

  public IRecord read() throws IOException {
    if (internalReader == null) {
      return null;
    }
    if (!internalReader.nextKeyValue()) {
      return null;
    }
    return internalReader.getCurrentValue();
  }

  @Override
  public void close() throws Exception {
    if (internalReader != null) {
      internalReader.close();
    }
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

      Map<String, String> extra = footer.getFileMetaData().getKeyValueMetaData();

      MessageType schema = footer.getFileMetaData().getSchema();
      // TODO projection schema
      MessageType requestedSchema = ProjectUtils.projectMessageType(schema, fields);

      if (skip) {
        return new IParquetReader(requestedSchema, extra);
      }

      ParquetFileReader reader = new ParquetFileReader(localInputfile, footer, options);
      reader.setRequestedSchema(requestedSchema);
      ParquetRecordReader<IRecord> internalReader =
          new ParquetRecordReader<>(new IRecordMaterializer(requestedSchema), reader, options);
      return new IParquetReader(internalReader, requestedSchema, extra);
    }

    public Builder project(Set<String> fields) {
      this.fields = fields;
      return this;
    }

    public Builder filter(Filter filter) {
      Pair<FilterPredicate, Boolean> filterPredicate = FilterUtils.toFilterPredicate(filter);
      if (filterPredicate.k != null) {
        optionsBuilder.withRecordFilter(FilterCompat.get(filterPredicate.k));
      } else {
        skip = !filterPredicate.v;
      }
      return this;
    }
  }
}
