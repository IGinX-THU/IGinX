package cn.edu.tsinghua.iginx.parquet.io.parquet.impl;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

public class IParquetReader implements AutoCloseable {
  private final ParquetReader<IRecord> internalReader;

  private final MessageType schema;

  private IParquetReader(ParquetReader<IRecord> internalReader, MessageType schema) {
    this.internalReader = internalReader;
    this.schema = schema;
  }

  private IParquetReader(MessageType schema) {
    this.internalReader = null;
    this.schema = schema;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public MessageType getSchema() {
    return schema;
  }

  public IRecord read() throws IOException {
    if (internalReader == null) {
      return null;
    }
    return internalReader.read();
  }

  @Override
  public void close() throws Exception {
    if (internalReader != null) {
      internalReader.close();
    }
  }

  public static class Builder {
    private final InternalBuilder internalBuilder;

    private final InputFile localInputfile;

    private boolean skip = false;

    public Builder(LocalInputFile localInputFile) {
      this.internalBuilder = new InternalBuilder(localInputFile);
      this.localInputfile = localInputFile;
    }

    public IParquetReader build() throws IOException {
      try (SeekableInputStream in = localInputfile.newStream()) {
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        ParquetMetadata footer = ParquetFileReader.readFooter(localInputfile, options, in);
        MessageType schema = footer.getFileMetaData().getSchema();
        if (skip) {
          return new IParquetReader(schema);
        }
        return new IParquetReader(internalBuilder.build(), schema);
      }
    }

    public Builder project(Set<String> fields) {
      // TODO
      return this;
    }

    public Builder filter(Filter filter) {
      Pair<FilterPredicate, Boolean> filterPredicate = FilterUtils.toFilterPredicate(filter);
      if (filterPredicate.k != null) {
        internalBuilder.withFilter(FilterCompat.get(filterPredicate.k)).useStatsFilter();
      } else {
        skip = !filterPredicate.v;
      }
      return this;
    }

    private static class InternalBuilder extends ParquetReader.Builder<IRecord> {

      private InternalBuilder(LocalInputFile path) {
        super(path);
      }

      @Override
      protected ReadSupport<IRecord> getReadSupport() {
        return new IReadSupport();
      }
    }
  }
}
