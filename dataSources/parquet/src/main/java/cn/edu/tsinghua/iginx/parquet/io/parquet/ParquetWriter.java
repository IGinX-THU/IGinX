package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.entity.DataChunk;
import cn.edu.tsinghua.iginx.parquet.io.FileMeta;
import cn.edu.tsinghua.iginx.parquet.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.parquet.column.ParquetProperties;

public class ParquetWriter implements FileWriter {
  public ParquetWriter(Path path, FileMeta meta, ParquetProperties properties) {}

  @Override
  public void seek(long start) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dump(Map<String, DataChunk> data, long limit) throws IOException {}

  @Override
  public void close() throws IOException {}
}
