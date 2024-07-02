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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.ConcatScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final DataBuffer<K, F, V> buffer;
  private final TableMeta<K, F, T, V> meta;

  public MemoryTable(DataBuffer<K, F, V> buffer, Map<F, T> types) {
    this.buffer = Objects.requireNonNull(buffer);
    Objects.requireNonNull(types);
    this.meta = new MemoryTableMeta<>(types, buffer.ranges());
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("buffer=" + buffer)
        .add("meta=" + meta)
        .toString();
  }

  @Nonnull
  @Override
  public TableMeta<K, F, T, V> getMeta() {
    return meta;
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges, @Nullable Filter predicate)
      throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("read {} where {} from {},{}", fields, ranges, buffer.fields(), buffer.ranges());
    }
    List<Scanner<K, Scanner<F, V>>> scanners = new ArrayList<>();
    for (Range<K> range : ranges.asRanges()) {
      scanners.add(buffer.scanRows(fields, range));
    }
    return new ConcatScanner<>(scanners.iterator());
  }

  public static class MemoryTableMeta<K extends Comparable<K>, F, T, V>
      implements TableMeta<K, F, T, V> {
    private final Map<F, T> schema;
    private final Map<F, Range<K>> ranges;

    public MemoryTableMeta(Map<F, T> schema, Map<F, Range<K>> ranges) {
      this.schema = schema;
      this.ranges = ranges;
    }

    public Map<F, T> getSchema() {
      return schema;
    }

    public Map<F, Range<K>> getRanges() {
      return ranges;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", MemoryTableMeta.class.getSimpleName() + "[", "]")
          .add("schema=" + schema)
          .add("ranges=" + ranges)
          .toString();
    }
  }
}
