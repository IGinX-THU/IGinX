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

package cn.edu.tsinghua.iginx.filestore.legacy.parquet.io.parquet;

import cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.exception.StorageException;
import java.io.IOException;
import java.nio.file.Path;
import shaded.iginx.org.apache.parquet.ParquetWriteOptions;
import shaded.iginx.org.apache.parquet.bytes.HeapByteBufferAllocator;
import shaded.iginx.org.apache.parquet.hadoop.CodecFactory;
import shaded.iginx.org.apache.parquet.hadoop.ParquetFileWriter;
import shaded.iginx.org.apache.parquet.hadoop.ParquetRecordWriter;
import shaded.iginx.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.LocalOutputFile;
import shaded.iginx.org.apache.parquet.io.OutputFile;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.TypeUtil;

public class IParquetWriter implements AutoCloseable {

  private final ParquetRecordWriter<IRecord> internalWriter;

  private final ParquetFileWriter fileWriter;

  IParquetWriter(ParquetRecordWriter<IRecord> internalWriter, ParquetFileWriter fileWriter) {
    this.internalWriter = internalWriter;
    this.fileWriter = fileWriter;
  }

  public static Builder builder(Path path, MessageType schema) {
    return builder(path, schema, 8 * 1024);
  }

  public static Builder builder(Path path, MessageType schema, int maxBufferSize) {
    return new Builder(
        new LocalOutputFile(path, new HeapByteBufferAllocator(), maxBufferSize), schema);
  }

  public void write(IRecord record) throws IOException {
    internalWriter.write(record);
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

    public Builder withCompressionCodec(String codec) {
      CompressionCodecName codecName = CompressionCodecName.valueOf(codec);
      optionsBuilder.withCompressor(new CodecFactory().getCompressor(codecName));
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
    record.sort();
    return record;
  }
}
