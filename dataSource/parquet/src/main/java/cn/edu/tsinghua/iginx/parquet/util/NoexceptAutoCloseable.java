package cn.edu.tsinghua.iginx.parquet.util;

public interface NoexceptAutoCloseable extends AutoCloseable {
  @Override
  void close();
}
