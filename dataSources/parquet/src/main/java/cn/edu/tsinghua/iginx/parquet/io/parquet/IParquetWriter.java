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
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import shaded.iginx.org.apache.parquet.ParquetWriteOptions;
import shaded.iginx.org.apache.parquet.bytes.ByteBufferAllocator;
import shaded.iginx.org.apache.parquet.bytes.HeapByteBufferAllocator;
import shaded.iginx.org.apache.parquet.compression.CompressionCodecFactory;
import shaded.iginx.org.apache.parquet.hadoop.CodecFactory;
import shaded.iginx.org.apache.parquet.hadoop.ParquetFileWriter;
import shaded.iginx.org.apache.parquet.hadoop.ParquetRecordWriter;
import shaded.iginx.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.LocalOutputFile;
import shaded.iginx.org.apache.parquet.io.OutputFile;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.TypeUtil;

public class IParquetWriter implements AutoCloseable {

  private final ParquetRecordWriter<IRecord> internalWriter;

  private final ParquetFileWriter fileWriter;

  IParquetWriter(ParquetRecordWriter<IRecord> internalWriter, ParquetFileWriter fileWriter) {
    this.internalWriter = internalWriter;
    this.fileWriter = fileWriter;
  }

  public static Builder builder(Path path, MessageType schema) {
    return new Builder(
        new LocalOutputFile(path, new HeapByteBufferAllocator(), Integer.MAX_VALUE), schema);
  }

  public static PrimitiveType getParquetType(
      String name, DataType type, Type.Repetition repetition) {
    switch (type) {
      case BOOLEAN:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, name);
      case INTEGER:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, name);
      case LONG:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, name);
      case FLOAT:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, name);
      case DOUBLE:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, name);
      case BINARY:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, name);
      default:
        throw new RuntimeException("Unsupported data type: " + type);
    }
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
      optionsBuilder.asParquetPropertiesBuilder().withPageSize(pageSize);
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

    public Builder withBufferAllocator(ByteBufferAllocator bufferAllocator) {
      optionsBuilder.asParquetPropertiesBuilder().withAllocator(bufferAllocator);
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
