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

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.local.ParquetFileWriter;
import org.apache.parquet.local.ParquetRecordWriter;
import org.apache.parquet.local.ParquetWriteOptions;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.TypeUtil;

public class IParquetWriter implements AutoCloseable {

  private final ParquetRecordWriter<IRecord> internalWriter;

  private final ParquetFileWriter fileWriter;

  IParquetWriter(ParquetRecordWriter<IRecord> internalWriter, ParquetFileWriter fileWriter) {
    this.internalWriter = internalWriter;
    this.fileWriter = fileWriter;
  }

  public static Builder builder(Path path, MessageType schema) {
    return new Builder(new LocalOutputFile(path), schema);
  }

  public void write(IRecord record) throws IOException {
    internalWriter.write(record);
  }

  @Override
  public void close() throws Exception {
    internalWriter.close();
  }

  public ParquetMetadata flush() throws IOException {
    try {
      internalWriter.close();
      return fileWriter.getFooter();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public static class Builder {

    private final ParquetWriteOptions.Builder optionsBuilder = ParquetWriteOptions.builder();

    private final OutputFile outputFile;

    private final MessageType schema;

    private final Map<String, String> extraMetaData = new HashMap<>();

    public Builder(OutputFile outputFile, MessageType schema) {
      this.outputFile = outputFile;
      this.schema = schema;
    }

    public IParquetWriter build() throws IOException {
      ParquetWriteOptions options = optionsBuilder.build();
      TypeUtil.checkValidWriteSchema(schema);
      ParquetFileWriter fileWriter = new ParquetFileWriter(outputFile, options);
      ParquetRecordWriter<IRecord> recordWriter =
          new ParquetRecordWriter<>(
              fileWriter, new IRecordDematerializer(schema), schema, extraMetaData, options);
      return new IParquetWriter(recordWriter, fileWriter);
    }

    public Builder withExtraMetaData(String key, String value) {
      extraMetaData.put(key, value);
      return this;
    }

    public Builder withRowGroupSize(long rowGroupSize) {
      optionsBuilder.withRowGroupSize(rowGroupSize);
      return this;
    }

    public Builder withPageSize(int pageSize) {
      optionsBuilder.withPageSize(pageSize);
      return this;
    }
  }

  public static IRecord getRecord(MessageType schema, Long key, Scanner<String, Object> value)
      throws StorageException {
    IRecord record = new IRecord();
    record.add(schema.getFieldIndex(Constants.KEY_FIELD_NAME), key);
    while (value.iterate()) {
      record.add(schema.getFieldIndex(value.key()), value.value());
    }
    return record;
  }
}
