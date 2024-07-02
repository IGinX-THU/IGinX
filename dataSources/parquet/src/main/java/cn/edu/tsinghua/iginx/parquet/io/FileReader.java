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
