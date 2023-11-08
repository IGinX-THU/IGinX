package cn.edu.tsinghua.iginx.parquet.io;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class IginxParquetWriter implements AutoCloseable {

  private final ParquetWriter<IginxRecord> internalWriter;

  IginxParquetWriter(ParquetWriter<IginxRecord> internalWriter) {
    this.internalWriter = internalWriter;
  }

  public static Builder builder(Path path, MessageType schema) {
    return new Builder(new LocalOutputFile(path), schema);
  }

  public void write(IginxRecord record) throws IOException {
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

    public IginxParquetWriter build() throws IOException {
      return new IginxParquetWriter(internalBuilder.build());
    }

    private static class InternalBuilder
        extends ParquetWriter.Builder<IginxRecord, InternalBuilder> {

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
      protected WriteSupport<IginxRecord> getWriteSupport(Configuration conf) {
        return new IginxWriteSupport(schema);
      }
    }
  }
}
