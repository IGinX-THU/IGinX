package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.io.*;
import cn.edu.tsinghua.iginx.parquet.io.common.EmptyReader;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.local.ParquetReadOptions;

public class ParquetFormat implements FileFormat {
  private final ParquetReadOptions options;

  private final ParquetProperties properties;

  public ParquetFormat(ParquetReadOptions options, ParquetProperties properties) {
    this.options = options;
    this.properties = properties;
  }

  @Override
  public boolean readIsSeekable() {
    return true;
  }

  @Override
  public boolean writeIsSeekable() {
    return false;
  }

  @Nonnull
  @Override
  public FileMeta getMeta(@Nonnull Path path) throws IOException {
    return new ParquetMeta(path, options);
  }

  @Nullable
  @Override
  public FileIndex getIndex(@Nonnull Path path, @Nonnull FileMeta meta) throws IOException {
    return new ParquetIndex(path, meta, options);
  }

  @Nonnull
  @Override
  public FileReader getReader(
      @Nonnull Path path, @Nonnull FileMeta meta, @Nonnull FileIndex index, @Nonnull Filter filter)
      throws IOException {
    if (!(meta instanceof ParquetMeta)) {
      throw new IOException("meta is not ParquetMeta");
    }
    ParquetMeta parquetMeta = (ParquetMeta) meta;
    if (!parquetMeta.path.equals(path)) {
      throw new IOException(String.format("meta path(%s) not match: %s", parquetMeta.path, path));
    }

    if (!(index instanceof ParquetIndex)) {
      throw new IOException("index is not ParquetIndex");
    }
    ParquetIndex parquetIndex = (ParquetIndex) index;
    if (!parquetIndex.path.equals(path)) {
      throw new IOException(String.format("index path(%s) not match: %s", parquetIndex.path, path));
    }

    ParquetReadOptions.Builder optionBuilder = ParquetReadOptions.builder().copy(this.options);

    Pair<FilterPredicate, Boolean> converted = FilterUtils.toFilterPredicate(filter);
    FilterPredicate parquetFilter = converted.getK();
    if (parquetFilter == null && !converted.getV()) {
      return EmptyReader.getInstance();
    }
    if (parquetFilter != null) {
      optionBuilder.withRecordFilter(FilterCompat.get(parquetFilter));
    }

    ParquetReadOptions options = optionBuilder.build();

    return new ParquetReader(path, parquetMeta, parquetIndex, options);
  }

  @Nonnull
  @Override
  public FileWriter getWriter(@Nonnull Path path, @Nonnull FileMeta meta) throws IOException {
    return new ParquetWriter(path, meta, properties);
  }
}
