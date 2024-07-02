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
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.EmptyScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EmptyTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final EmptyTable<?, ?, ?, ?> EMPTY = new EmptyTable<>();

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, F, T, V> EmptyTable<K, F, T, V> getInstance() {
    return (EmptyTable<K, F, T, V>) EMPTY;
  }

  @Nonnull
  @Override
  public MemoryTable.MemoryTableMeta<K, F, T, V> getMeta() {
    return new MemoryTable.MemoryTableMeta<>(new HashMap<>(), new HashMap<>());
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges, @Nullable Filter predicate)
      throws IOException {
    return EmptyScanner.getInstance();
  }
}
