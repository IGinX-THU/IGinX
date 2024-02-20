package cn.edu.tsinghua.iginx.parquet.db.lsm.index;

import cn.edu.tsinghua.iginx.parquet.shared.exception.NotIntegrityException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FieldIndex<K extends Comparable<K>, F, T, V> {
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final T type;
  private final Map<String, Range<K>> tableRange = new HashMap<>();

  public FieldIndex(T type) {
    this.type = type;
  }

  public T getType() {
    return type;
  }

  public void addTable(String name, Range<K> range) {
    lock.writeLock().lock();
    try {
      if (this.tableRange.containsKey(name)) {
        throw new NotIntegrityException("table " + name + " already exists");
      }
      this.tableRange.put(name, range);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<String> find(RangeSet<K> ranges) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<String, Range<K>> entry : tableRange.entrySet()) {
        if (ranges.intersects(entry.getValue())) {
          result.add(entry.getKey());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public Set<String> find() {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      result.addAll(tableRange.keySet());
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void delete(RangeSet<K> ranges) {
    lock.writeLock().lock();
    try {
      Iterator<Map.Entry<String, Range<K>>> iterator = tableRange.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Range<K>> entry = iterator.next();
        if (!ranges.intersects(entry.getValue())) {
          continue;
        }
        if (ranges.encloses(entry.getValue())) {
          iterator.remove();
        } else {
          tableRange.put(entry.getKey(), ranges.subRangeSet(entry.getValue()).span());
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public RangeSet<K> ranges() {
    ImmutableRangeSet.Builder<K> builder = ImmutableRangeSet.builder();
    lock.readLock().lock();
    try {
      for (Range<K> range : tableRange.values()) {
        builder.add(range);
      }
    } finally {
      lock.readLock().unlock();
    }
    return builder.build();
  }
}
