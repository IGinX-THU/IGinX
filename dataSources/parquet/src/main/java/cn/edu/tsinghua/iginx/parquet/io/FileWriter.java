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
