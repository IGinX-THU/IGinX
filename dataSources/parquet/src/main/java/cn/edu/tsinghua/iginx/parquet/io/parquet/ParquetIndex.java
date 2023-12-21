package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.entity.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import cn.edu.tsinghua.iginx.parquet.io.FileIndex;
import cn.edu.tsinghua.iginx.parquet.io.FileMeta;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.parquet.local.ParquetReadOptions;

public class ParquetIndex implements FileIndex {
  final Path path;

  public ParquetIndex(Path path, FileMeta meta, ParquetReadOptions options) {
    this.path = path;
  }

  @Override
  public long size() {
    return 0;
  }

  @Nonnull
  @Override
  public Scanner<Long, Long> detect(@Nonnull Filter filter) throws IOException {
    return new IteratorScanner<>(
        Collections.singletonMap(0L, Long.MAX_VALUE).entrySet().iterator());
  }
}
