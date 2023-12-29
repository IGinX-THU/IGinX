package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.nio.file.Path;
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
      MessageType requestedSchema = schema;

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
      // TODO
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
