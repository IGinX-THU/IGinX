package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FileFormat {

  boolean readIsSeekable();

  boolean writeIsSeekable();

  /**
   * load meta data from file
   *
   * @param path file path
   * @return meta data
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  FileMeta getMeta(@Nonnull Path path) throws IOException;

  /**
   * load data index from file
   *
   * @param path file path
   * @return data index
   * @throws IOException if an I/O error occurs
   */
  @Nullable
  FileIndex getIndex(@Nonnull Path path, @Nonnull FileMeta meta) throws IOException;

  /**
   * get reader of specified file
   *
   * @param path file path
   * @param meta meta data loaded from file
   * @param index data index loaded from file
   * @param filter filter to apply
   * @return file reader
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  FileReader getReader(
      @Nonnull Path path, @Nonnull FileMeta meta, @Nonnull FileIndex index, @Nonnull Filter filter)
      throws IOException;

  /**
   * get writer of specified file
   *
   * @param path file path
   * @param meta meta data to write
   * @return file writer
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  FileWriter getWriter(@Nonnull Path path, @Nonnull FileMeta meta) throws IOException;
}
