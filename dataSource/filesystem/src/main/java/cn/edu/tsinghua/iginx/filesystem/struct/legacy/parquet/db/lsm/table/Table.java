/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.LazyRowScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception.StorageException;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

public interface Table {

  TableMeta getMeta() throws IOException;

  Scanner<Long, Scanner<String, Object>> scan(
      Set<String> fields, RangeSet<Long> range, @Nullable Filter superSetPredicate)
      throws IOException;

  default Scanner<Long, Scanner<String, Object>> scan(Set<String> fields, RangeSet<Long> ranges)
      throws IOException {
    return scan(fields, ranges, null);
  }

  default Scanner<Long, Scanner<String, Object>> lazyScan(
      Set<String> fields, RangeSet<Long> ranges) {
    return new LazyRowScanner<>(
        () -> {
          try {
            return scan(fields, ranges);
          } catch (IOException e) {
            throw new StorageException(e);
          }
        });
  }
}
