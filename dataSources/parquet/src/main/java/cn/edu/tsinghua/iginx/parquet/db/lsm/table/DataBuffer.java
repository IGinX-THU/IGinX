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

package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.ColumnUnionRowScanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class DataBuffer<K extends Comparable<K>, F, V> {

  private final Function<F, NavigableMap<K, V>> COLUMN_FACTORY = k -> new ConcurrentSkipListMap<>();

  private final Map<F, NavigableMap<K, V>> data = new ConcurrentHashMap<>();

  public void putRows(Scanner<K, Scanner<F, V>> scanner) throws StorageException {
    while (scanner.iterate()) {
      K key = scanner.key();
      try (Scanner<F, V> row = scanner.value()) {
        while (row.iterate()) {
          F field = row.key();
          V value = row.value();
          NavigableMap<K, V> column = data.computeIfAbsent(field, COLUMN_FACTORY);
          column.put(key, value);
        }
      } catch (Exception e) {
        throw new StorageException(e);
      }
    }
  }

  public void putColumns(Scanner<F, Scanner<K, V>> scanner) throws StorageException {
    while (scanner.iterate()) {
      F field = scanner.key();
      try (Scanner<K, V> column = scanner.value()) {
        while (column.iterate()) {
          K key = column.key();
          V value = column.value();
          NavigableMap<K, V> columnMap = data.computeIfAbsent(field, COLUMN_FACTORY);
          columnMap.put(key, value);
        }
      }
    }
  }

  public void remove(@Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges) {
    for (F deletedField : fields) {
      BiFunction<F, NavigableMap<K, V>, NavigableMap<K, V>> removeFunction =
          (field, column) -> {
            for (com.google.common.collect.Range<K> range : ranges.asRanges()) {
              subMapRefOf(column, range).clear();
            }
            return column;
          };
      data.computeIfPresent(deletedField, removeFunction);
    }
  }

  public void remove(@Nonnull RangeSet<K> ranges) {
    for (NavigableMap<K, V> column : data.values()) {
      for (com.google.common.collect.Range<K> range : ranges.asRanges()) {
        subMapRefOf(column, range).clear();
      }
    }
  }

  public void remove(@Nonnull Set<F> fields) {
    for (F deletedField : fields) {
      data.remove(deletedField);
    }
  }

  public void clear() {
    data.clear();
  }

  private static <K extends Comparable<K>, V> NavigableMap<K, V> subMapRefOf(
      NavigableMap<K, V> column, Range<K> range) {
    if (range.encloses(Range.all())) {
      return column;
    } else if (!range.hasLowerBound()) {
      return column.headMap(range.upperEndpoint(), range.upperBoundType() == BoundType.CLOSED);
    } else if (!range.hasUpperBound()) {
      return column.tailMap(range.lowerEndpoint(), range.lowerBoundType() == BoundType.CLOSED);
    } else {
      return column.subMap(
          range.lowerEndpoint(),
          range.lowerBoundType() == BoundType.CLOSED,
          range.upperEndpoint(),
          range.upperBoundType() == BoundType.CLOSED);
    }
  }

  @Nonnull
  public Set<F> fields() {
    return Collections.unmodifiableSet(new HashSet<>(data.keySet()));
  }

  @Nonnull
  public Map<F, Range<K>> ranges() {
    Map<F, Range<K>> ranges = new HashMap<>();
    for (Map.Entry<F, NavigableMap<K, V>> entry : data.entrySet()) {
      NavigableMap<K, V> column = entry.getValue();

      Map.Entry<K, V> firstEntry = column.firstEntry();
      Map.Entry<K, V> lastEntry = column.lastEntry();

      if (firstEntry != null && lastEntry != null) {
        ranges.put(entry.getKey(), Range.closed(firstEntry.getKey(), lastEntry.getKey()));
      }
    }
    return ranges;
  }

  @Nonnull
  public Scanner<K, Scanner<F, V>> scanRows(@Nonnull Set<F> fields, @Nonnull Range<K> range) {
    Map<F, Scanner<K, V>> scanners = subScanners(fields, range);
    return new ColumnUnionRowScanner<>(scanners);
  }

  public Scanner<F, Scanner<K, V>> scanColumns(@Nonnull Set<F> fields, @Nonnull Range<K> range) {
    Map<F, Scanner<K, V>> scanners = subScanners(fields, range);
    return new IteratorScanner<>(scanners.entrySet().iterator());
  }

  private Map<F, Scanner<K, V>> subScanners(@Nonnull Set<F> fields, @Nonnull Range<K> range) {
    Map<F, Scanner<K, V>> scanners = new HashMap<>();
    for (F field : fields) {
      NavigableMap<K, V> column = data.get(field);
      if (column != null) {
        NavigableMap<K, V> subColumn = subMapRefOf(column, range);
        Scanner<K, V> subColumnScanner = new IteratorScanner<>(subColumn.entrySet().iterator());
        scanners.put(field, subColumnScanner);
      }
    }
    return scanners;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", DataBuffer.class.getSimpleName() + "{", "}")
        .add("data=" + data)
        .toString();
  }

  public void close() throws StorageException {
    data.clear();
  }
}
