package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class WriteBuffer<K extends Comparable<K>, F, V> {

  private final Function<F, ConcurrentSkipListMap<K, V>> COLUMN_FACTORY =
      k -> new ConcurrentSkipListMap<>();

  private final Function<F, RangeSet<K>> RANGE_SET_FACTORY = k -> new RangeSet<>();

  private final ConcurrentHashMap<F, ConcurrentSkipListMap<K, V>> data = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock deleteLock = new ReentrantReadWriteLock();

  private final Map<F, RangeSet<K>> deletedRanges = new HashMap<>();

  public void putRows(
      cn.edu.tsinghua.iginx.parquet.entity.Scanner<
              K, cn.edu.tsinghua.iginx.parquet.entity.Scanner<F, V>>
          scanner)
      throws NativeStorageException {
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

  public void putColumns(
      cn.edu.tsinghua.iginx.parquet.entity.Scanner<
              F, cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V>>
          scanner)
      throws NativeStorageException {
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

  public void remove(@Nonnull Set<F> fields, RangeSet<K> ranges) {
    for (F deletedField : fields) {
      data.computeIfPresent(
          deletedField,
          (field, column) -> {
            for (Range<K> range : ranges) {
              subMapRefOf(column, range).clear();
            }
            return column;
          });
    }
    deleteLock.writeLock().lock();
    try {
      for (F deletedField : fields) {
        deletedRanges.computeIfAbsent(deletedField, RANGE_SET_FACTORY).addAll(ranges);
      }
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  public void remove(RangeSet<K> ranges) {
    for (NavigableMap<K, V> column : data.values()) {
      for (Range<K> range : ranges) {
        subMapRefOf(column, range).clear();
      }
    }
    deleteLock.writeLock().lock();
    try {
      deletedRanges.computeIfAbsent(null, RANGE_SET_FACTORY).addAll(ranges);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  public void remove(@Nonnull Set<F> fields) {
    for (F deletedField : fields) {
      data.remove(deletedField);
    }
    deleteLock.writeLock().lock();
    try {
      for (F deletedField : fields) {
        deletedRanges.computeIfAbsent(deletedField, RANGE_SET_FACTORY).add(new Range<>(null, null));
      }
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  public void remove() {
    data.clear();
    deleteLock.writeLock().lock();
    try {
      deletedRanges.clear();
      deletedRanges.computeIfAbsent(null, RANGE_SET_FACTORY).add(new Range<>(null, null));
    } finally {
      deleteLock.writeLock().unlock();
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

  @SuppressWarnings("unchecked")
  public Map<F, RangeSet<K>> cloneDeletedRanges() {
    try {
      deleteLock.readLock().lock();
      Map<F, RangeSet<K>> result = new HashMap<>();
      for (Map.Entry<F, RangeSet<K>> entry : deletedRanges.entrySet()) {
        result.put(entry.getKey(), (RangeSet<K>) entry.getValue().clone());
      }
      return result;
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Nonnull
  public Set<F> fields() {
    return Collections.unmodifiableSet(new HashSet<>(data.keySet()));
  }

  @Nonnull
  public Range<K> range() {
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

    return new Range<>(min, max);
  }

  @Nonnull
  public cn.edu.tsinghua.iginx.parquet.entity.Scanner<
          K, cn.edu.tsinghua.iginx.parquet.entity.Scanner<F, V>>
      scanRows(Set<F> fields, Range<K> range) throws NativeStorageException {
    Map<F, cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V>> scanners = new HashMap<>();
    for (F field : fields) {
      NavigableMap<K, V> column = data.get(field);
      if (column != null) {
        NavigableMap<K, V> subColumn = subMapRefOf(column, range);
        cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V> subColumnScanner =
            new IteratorScanner<>(subColumn.entrySet().iterator());
        scanners.put(field, subColumnScanner);
      }
    }
    return new ColumnUnionRowScanner<>(scanners);
  }

  public cn.edu.tsinghua.iginx.parquet.entity.Scanner<
          F, cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V>>
      scanColumns(Set<F> fields, Range<K> range) throws NativeStorageException {
    Map<F, cn.edu.tsinghua.iginx.parquet.entity.Scanner<K, V>> scanners = new HashMap<>();
    for (F field : fields) {
      NavigableMap<K, V> column = data.get(field);
      if (column != null) {
        NavigableMap<K, V> subColumn = subMapRefOf(column, range);
        Scanner<K, V> subColumnScanner = new IteratorScanner<>(subColumn.entrySet().iterator());
        scanners.put(field, subColumnScanner);
      }
    }
    return new IteratorScanner<>(scanners.entrySet().iterator());
  }

  public void close() throws NativeStorageException {
    data.clear();
    deletedRanges.clear();
  }
}
