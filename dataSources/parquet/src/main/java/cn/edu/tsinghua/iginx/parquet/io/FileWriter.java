package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.io.common.DataChunk;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface FileWriter extends Closeable {

  /**
   * start row offset to read
   *
   * @param start start row offset to read
   */
  void seek(long start) throws IOException;

  /**
   * dump data chunks into current row offset
   *
   * @param data data chunks to dump. key is field name, value is data chunk. row number of each
   *     data chunk must be same.
   */
  void dump(Map<String, DataChunk> data, long limit) throws IOException;
}
