/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
