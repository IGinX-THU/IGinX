package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
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

  public void putRows(Scanner<K, Scanner<F, V>> scanner) throws NativeStorageException {
    while (scanner.iterate()) {
      K key = scanner.key();
      try (cn.edu.tsinghua.iginx.parquet.entity.Scanner<F, V> row = scanner.value()) {
        while (row.iterate()) {
          F field = row.key();
          V value = row.value();
          NavigableMap<K, V> column = data.computeIfAbsent(field, COLUMN_FACTORY);
          column.put(key, value);
        }
      }
    }
  }

  public void putColumns(Scanner<F, Scanner<K, V>> scanner) throws NativeStorageException {
    while (scanner.iterate()) {
      F field = scanner.key();
      try (cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V> column = scanner.value()) {
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

  public void remove() {
    data.clear();
  }

  private static <K extends Comparable<K>, V> NavigableMap<K, V> subMapRefOf(
      NavigableMap<K, V> column, com.google.common.collect.Range<K> range) {
    if (range.encloses(com.google.common.collect.Range.all())) {
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

  private static <K extends Comparable<K>, V> NavigableMap<K, V> subMapRefOf(
      NavigableMap<K, V> column, Range<K> range) {
    if (range.isFull()) {
      return column;
    } else if (range.first() == null) {
      return column.headMap(range.last(), true);
    } else if (range.last() == null) {
      return column.tailMap(range.first(), true);
    } else {
      return column.subMap(range.first(), true, range.last(), true);
    }
  }

  @Nonnull
  public Set<F> fields() {
    return Collections.unmodifiableSet(new HashSet<>(data.keySet()));
  }

  @Nonnull
  public RangeSet<K> ranges() {
    K min =
        data.values().stream()
            .filter(Objects::nonNull)
            .map(NavigableMap::firstEntry)
            .filter(Objects::nonNull)
            .map(Map.Entry::getKey)
            .min(Comparator.naturalOrder())
            .orElse(null);

    K max =
        data.values().stream()
            .filter(Objects::nonNull)
            .map(NavigableMap::lastEntry)
            .filter(Objects::nonNull)
            .map(Map.Entry::getKey)
            .max(Comparator.naturalOrder())
            .orElse(null);
    if (min == null || max == null) {
      return ImmutableRangeSet.of();
    }
    return ImmutableRangeSet.of(com.google.common.collect.Range.closed(min, max));
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

  public void close() throws NativeStorageException {
    data.clear();
  }
}
