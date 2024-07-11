package cn.edu.tsinghua.iginx.parquet.util;

public interface Awaitable {
  void await() throws InterruptedException;
}
