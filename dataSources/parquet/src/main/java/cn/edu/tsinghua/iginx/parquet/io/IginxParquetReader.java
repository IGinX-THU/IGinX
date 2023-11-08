package cn.edu.tsinghua.iginx.parquet.io;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

public class IginxParquetReader implements AutoCloseable {
  private final ParquetReader<IginxRecord> internalReader;

  private final MessageType schema;

  public IginxParquetReader(ParquetReader<IginxRecord> internalReader, MessageType schema) {
    this.internalReader = internalReader;
    this.schema = schema;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public MessageType getSchema() {
    return schema;
  }

  public IginxRecord read() throws IOException {
    return internalReader.read();
  }

  @Override
  public void close() throws Exception {
    internalReader.close();
  }

  public static class Builder {
    private final InternalBuilder internalBuilder;

    private final InputFile localInputfile;

    public Builder(LocalInputFile localInputFile) {
      this.internalBuilder = new InternalBuilder(localInputFile);
      this.localInputfile = localInputFile;
    }

    public IginxParquetReader build() throws IOException {
      try (SeekableInputStream in = localInputfile.newStream()) {
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        ParquetMetadata footer = ParquetFileReader.readFooter(localInputfile, options, in);
        MessageType schema = footer.getFileMetaData().getSchema();
        return new IginxParquetReader(internalBuilder.build(), schema);
      }
    }

    private static class InternalBuilder extends ParquetReader.Builder<IginxRecord> {

      private InternalBuilder(LocalInputFile path) {
        super(path);
      }

      @Override
      protected ReadSupport<IginxRecord> getReadSupport() {
        return new IginxReadSupport();
      }
    }
  }
}
