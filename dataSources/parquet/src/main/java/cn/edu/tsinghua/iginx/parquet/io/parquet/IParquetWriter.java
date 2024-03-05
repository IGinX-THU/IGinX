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

import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.format.parquet.ParquetRecordWriter;
import cn.edu.tsinghua.iginx.format.parquet.ParquetWriteOptions;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
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

  public void setExtraMetaData(String key, String value) {
    internalWriter.setExtraMetaData(key, value);
  }

  @Override
  public void close() throws Exception {
    internalWriter.close();
  }

  public ParquetMetadata flush() throws IOException {
    internalWriter.close();
    return fileWriter.getFooter();
  }

  public static class Builder {

    private final ParquetWriteOptions.Builder optionsBuilder = ParquetWriteOptions.builder();

    private final OutputFile outputFile;

    private final MessageType schema;

    public Builder(OutputFile outputFile, MessageType schema) {
      this.outputFile = outputFile;
      this.schema = schema;
    }

    public IParquetWriter build() throws IOException {
      ParquetWriteOptions options = optionsBuilder.build();
      TypeUtil.checkValidWriteSchema(schema);
      ParquetFileWriter fileWriter = new ParquetFileWriter(outputFile, options);
      ParquetRecordWriter<IRecord> recordWriter =
          new ParquetRecordWriter<>(fileWriter, new IRecordDematerializer(schema), options);
      return new IParquetWriter(recordWriter, fileWriter);
    }

    public Builder withRowGroupSize(long rowGroupSize) {
      optionsBuilder.withRowGroupSize(rowGroupSize);
      return this;
    }

    public Builder withPageSize(int pageSize) {
      optionsBuilder.withPageSize(pageSize);
      return this;
    }

    public Builder withCompressor(String name, int zstdLevel, int zstdWorkers) {
      CompressionCodecName codecName = CompressionCodecName.fromConf(name);
      CompressionCodecFactory.BytesInputCompressor compressor =
          (new CodecFactory(CodecFactory.DEFAULT_LZ4_SEGMENT_SIZE, zstdLevel, zstdWorkers))
              .getCompressor(codecName);

      optionsBuilder.withCompressor(compressor);
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
