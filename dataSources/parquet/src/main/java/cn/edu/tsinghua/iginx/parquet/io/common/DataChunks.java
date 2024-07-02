package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.parquet.db.util.iterator.EmptyScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import javax.annotation.Nonnull;

public class DataChunks {
  private DataChunks() {}

  private static class EmptyDataChunk implements DataChunk {

    @Override
    public long bytes() {
      return 0;
    }

    @Nonnull
    @Override
    public Scanner<Long, Object> scan(long position) {
      return EmptyScanner.getInstance();
    }

    @Override
    public void close() throws Exception {}
  }

  private static final DataChunk EMPTY = new EmptyDataChunk();

  public static DataChunk empty() {
    return EMPTY;
  }
}
