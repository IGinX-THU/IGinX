package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import javax.annotation.Nonnull;

public interface DataChunk extends AutoCloseable {

  /**
   * get the size of this data chunk
   *
   * @return size of this data chunk in bytes
   */
  long bytes();

  /**
   * get the scanner of this data chunk
   *
   * @param start begin row offset
   * @return scanner of this data chunk
   */
  @Nonnull
  Scanner<Long, Object> scan(long start);
}
