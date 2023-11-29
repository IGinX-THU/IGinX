package cn.edu.tsinghua.iginx.parquet.io.parquet.impl;

import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import cn.edu.tsinghua.iginx.parquet.entity.Constants;
import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class IParquetWriter implements AutoCloseable {

  private final ParquetWriter<IRecord> internalWriter;

  IParquetWriter(ParquetWriter<IRecord> internalWriter) {
    this.internalWriter = internalWriter;
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

  public static class Builder {
    private final InternalBuilder internalBuilder;

    public Builder(LocalOutputFile localOutputFile, MessageType schema) {
      this.internalBuilder = new InternalBuilder(localOutputFile, schema);
    }

    public IParquetWriter build() throws IOException {
      return new IParquetWriter(internalBuilder.build());
    }

    public Builder withWriteMode(ParquetFileWriter.Mode mode) {
      internalBuilder.withWriteMode(mode);
      return this;
    }

    public Builder withRowGroupSize(long rowGroupSize) {
      internalBuilder.withRowGroupSize(rowGroupSize);
      return this;
    }

    public Builder withPageSize(int pageSize) {
      internalBuilder.withPageSize(pageSize);
      return this;
    }

    private static class InternalBuilder extends ParquetWriter.Builder<IRecord, InternalBuilder> {

      private final MessageType schema;

      private InternalBuilder(OutputFile path, MessageType schema) {
        super(path);
        withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
        this.schema = schema;
      }

      @Override
      protected InternalBuilder self() {
        return this;
      }

      @Override
      protected WriteSupport<IRecord> getWriteSupport(Configuration conf) {
        return new IWriteSupport(schema);
      }
    }
  }

  public static IRecord getRecord(MessageType schema, Long key, Scanner<String, Object> value)
      throws NativeStorageException {
    IRecord record = new IRecord();
    record.add(schema.getFieldIndex(Constants.KEY_FIELD_NAME), key);
    while (value.iterate()) {
      record.add(schema.getFieldIndex(value.key()), value.value());
    }
    return record;
  }
}
