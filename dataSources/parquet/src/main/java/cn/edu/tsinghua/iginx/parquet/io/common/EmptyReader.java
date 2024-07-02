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
package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.parquet.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

public class EmptyReader implements FileReader {
  private EmptyReader() {}

  public static EmptyReader getInstance() {
    return EmptyReader.INSTANCE;
  }

  private static final EmptyReader INSTANCE = new EmptyReader();

  @Nonnull
  @Override
  public Map<String, DataChunk> load(@Nonnull Set<String> fields, long limit) throws IOException {
    Map<String, DataChunk> result = new HashMap<>();
    for (String field : fields) {
      result.put(field, DataChunks.empty());
    }
    return result;
  }

  @Override
  public void seek(long start) throws IOException {
    throw new IOException("EmptyReader is not seekable");
  }

  @Override
  public void close() throws IOException {}
}
