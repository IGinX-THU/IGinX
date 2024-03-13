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

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import cn.edu.tsinghua.iginx.format.parquet.api.RecordDematerializer;
import cn.edu.tsinghua.iginx.format.parquet.codec.DefaultCodecFactory;
import cn.edu.tsinghua.iginx.format.parquet.io.LocalOutputFile;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import shaded.iginx.org.apache.parquet.bytes.ByteBufferAllocator;
import shaded.iginx.org.apache.parquet.bytes.HeapByteBufferAllocator;
import shaded.iginx.org.apache.parquet.compression.CompressionCodecFactory;
import shaded.iginx.org.apache.parquet.hadoop.ExportedParquetRecordWriter;
import shaded.iginx.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import shaded.iginx.org.apache.parquet.io.OutputFile;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class IParquetWriter extends ParquetWriter<IRecord> {

  private IParquetWriter(ExportedParquetRecordWriter<IRecord> internalWriter) {
    super(internalWriter);
  }

  public static Builder builder(
      Path path, MessageType schema, ByteBufferAllocator fileBufferAllocator) {
    return new Builder(new LocalOutputFile(path, fileBufferAllocator, Integer.MAX_VALUE), schema);
  }

  public static Builder builder(Path path, MessageType schema) {
    return builder(path, schema, new HeapByteBufferAllocator());
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

  public static class Builder extends ParquetWriter.Builder<IRecord, IParquetWriter, Builder> {

    private final OutputFile outputFile;

    private final MessageType schema;

    public Builder(OutputFile outputFile, MessageType schema) {
      this.outputFile = outputFile;
      this.schema = schema;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected RecordDematerializer<IRecord> dematerializer() throws IOException {
      return new IRecordDematerializer(schema);
    }

    @Override
    public IParquetWriter build() throws IOException {
      ExportedParquetRecordWriter<IRecord> internalWriter =
          build(outputFile, schema, Collections.emptyMap());
      return new IParquetWriter(internalWriter);
    }

    public Builder withCodec(String name) {
      CompressionCodecName codecName = CompressionCodecName.fromConf(name);
      return super.withCodec(codecName);
    }

    public Builder withCodecFactory(
        ByteBufferAllocator compressAllocator, int zstdLevel, int zstdWorkers) {
      CompressionCodecFactory codecFactory =
          new DefaultCodecFactory(
              compressAllocator,
              DefaultCodecFactory.DEFAULT_LZ4_SEGMENT_SIZE,
              zstdLevel,
              zstdWorkers);
      return super.withCodecFactory(codecFactory);
    }
  }

  public static IRecord getRecord(MessageType schema, Long key, Scanner<String, Object> rowScanner)
      throws IOException {
    Map<String, Object> row = new HashMap<>();
    row.put(Constants.KEY_FIELD_NAME, key);
    while (rowScanner.iterate()) {
      row.put(rowScanner.key(), rowScanner.value());
    }
    IRecord record = new IRecord();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      String fieldName = schema.getFields().get(i).getName();
      Object value = row.get(fieldName);
      if (value != null) {
        record.add(i, value);
      }
    }
    return record;
  }
}
