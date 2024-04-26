/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
