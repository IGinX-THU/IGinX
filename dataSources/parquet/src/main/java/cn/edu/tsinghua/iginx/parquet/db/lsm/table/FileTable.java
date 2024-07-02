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
package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileTable.class);
  private final String tableName;

  private final ReadWriter<K, F, T, V> readWriter;

  public FileTable(String tableName, ReadWriter<K, F, T, V> readWriter) {
    this.tableName = tableName;
    this.readWriter = readWriter;
  }

  @Override
  @Nonnull
  public TableMeta<K, F, T, V> getMeta() throws IOException {
    return readWriter.readMeta(tableName);
  }

  @Override
  @Nonnull
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges, @Nullable Filter predicate)
      throws IOException {
    LOGGER.debug("read {} where {} & {} from {}", fields, ranges, predicate, tableName);
    return readWriter.scanData(tableName, fields, ranges, predicate);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FileTable.class.getSimpleName() + "[", "]")
        .add("tableName='" + tableName + "'")
        .add("readWriter=" + readWriter)
        .toString();
  }
}
