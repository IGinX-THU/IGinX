package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.io.common.DataChunk;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FileReader extends Closeable {

  /**
   * set start row offset to read
   *
   * @param start start row offset to read
   */
  void seek(long start) throws IOException;

  /**
   * load data chunks in current row offset
   *
   * @param fields fields to load. empty if all fields
   * @param limit max number of the row to load
   * @return data chunks of each field, null if reader has no more data
   */
  @Nullable
  Map<String, DataChunk> load(@Nonnull Set<String> fields, long limit) throws IOException;
}
