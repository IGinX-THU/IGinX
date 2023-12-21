package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.entity.DataChunk;
import cn.edu.tsinghua.iginx.parquet.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.parquet.local.ParquetReadOptions;

class ParquetReader implements FileReader {

  public ParquetReader(
      Path path, ParquetMeta meta, ParquetIndex index, ParquetReadOptions options) {}

  @Nonnull
  @Override
  public Map<String, DataChunk> load(@Nonnull Set<String> fields, long limit) throws IOException {
    return null;
  }

  @Override
  public void seek(long start) throws IOException {}

  @Override
  public void close() throws IOException {}
}
