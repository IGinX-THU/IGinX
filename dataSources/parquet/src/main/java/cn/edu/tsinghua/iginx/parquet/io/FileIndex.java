package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import java.io.IOException;
import javax.annotation.Nonnull;

public interface FileIndex {

  /**
   * estimate size of data index
   *
   * @return size of data index in bytes
   */
  long size();

  /**
   * detect row ranges
   *
   * @param filter filter
   * @return row ranges. key is start row offset, value is row number.
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  Scanner<Long, Long> detect(@Nonnull Filter filter) throws IOException;
}
