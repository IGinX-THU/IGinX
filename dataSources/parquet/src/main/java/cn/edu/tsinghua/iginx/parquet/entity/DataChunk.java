package cn.edu.tsinghua.iginx.parquet.entity;

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
