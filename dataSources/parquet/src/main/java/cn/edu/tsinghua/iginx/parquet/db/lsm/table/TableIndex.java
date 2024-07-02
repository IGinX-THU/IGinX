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

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.TypeConflictedException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableIndex<K extends Comparable<K>, F, T, V> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableIndex.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final Map<F, FieldIndex<K, F, T, V>> indexes = new HashMap<>();

  public TableIndex(TableStorage<K, F, T, V> tableStorage) {
    try {
      for (String tableName : tableStorage.tableNames()) {
        TableMeta<K, F, T, V> meta = tableStorage.get(tableName).getMeta();
        declareFields(meta.getSchema());
        addTable(tableName, meta);
      }
    } catch (IOException | TypeConflictedException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public Set<String> find(AreaSet<K, F> areas) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      RangeSet<K> rangeSet = areas.getKeys();
      if (!rangeSet.isEmpty()) {
        for (FieldIndex<K, F, T, V> fieldIndex : indexes.values()) {
          Set<String> tables = fieldIndex.find(rangeSet);
          result.addAll(tables);
        }
      }
      for (F field : areas.getFields()) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          continue;
        }
        Set<String> tables = fieldIndex.find();
        result.addAll(tables);
      }
      for (Map.Entry<F, RangeSet<K>> entry : areas.getSegments().entrySet()) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(entry.getKey());
        if (fieldIndex == null) {
          continue;
        }
        Set<String> tables = fieldIndex.find(entry.getValue());
        result.addAll(tables);
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public Map<F, Range<K>> ranges() {
    Map<F, Range<K>> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<F, FieldIndex<K, F, T, V>> entry : indexes.entrySet()) {
        RangeSet<K> rangeSet = entry.getValue().ranges();
        if (!rangeSet.isEmpty()) {
          result.put(entry.getKey(), rangeSet.span());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void declareFields(Map<F, T> schema) throws TypeConflictedException {
    boolean hasNewField = checkOldFields(schema);
    if (hasNewField) {
      declareNewFields(schema);
    }
  }

  private boolean checkOldFields(Map<F, T> schema) throws TypeConflictedException {
    boolean hasNewField = false;
    lock.readLock().lock();
    try {
      for (Map.Entry<F, T> entry : schema.entrySet()) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(entry.getKey());
        if (fieldIndex == null) {
          hasNewField = true;
        } else if (!fieldIndex.getType().equals(entry.getValue())) {
          throw new TypeConflictedException(
              entry.getKey().toString(),
              entry.getValue().toString(),
              fieldIndex.getType().toString());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return hasNewField;
  }

  private void declareNewFields(Map<F, T> schema) throws TypeConflictedException {
    Map<F, T> newSchema = new HashMap<>();
    lock.writeLock().lock();
    try {
      for (Map.Entry<F, T> entry : schema.entrySet()) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(entry.getKey());
        if (fieldIndex == null) {
          newSchema.put(entry.getKey(), entry.getValue());
        } else if (!fieldIndex.getType().equals(entry.getValue())) {
          throw new TypeConflictedException(
              entry.getKey().toString(),
              entry.getValue().toString(),
              fieldIndex.getType().toString());
        }
      }
      LOGGER.debug("declare new fields: {}", newSchema);
      for (Map.Entry<F, T> entry : newSchema.entrySet()) {
        indexes.put(entry.getKey(), new FieldIndex<>(entry.getValue()));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<F, T> getType(Set<F> fields, Consumer<F> processMissingField) {
    Map<F, T> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (F field : fields) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          processMissingField.accept(field);
        } else {
          result.put(field, fieldIndex.getType());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public Map<F, T> getType() {
    Map<F, T> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<F, FieldIndex<K, F, T, V>> entry : indexes.entrySet()) {
        result.put(entry.getKey(), entry.getValue().getType());
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void addTable(String name, TableMeta<K, F, T, V> meta) {
    lock.readLock().lock();
    try {
      Map<F, T> types = meta.getSchema();
      for (Map.Entry<F, Range<K>> entry : meta.getRanges().entrySet()) {
        F field = entry.getKey();
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          throw new NotIntegrityException("field " + field + " is not found in schema");
        }
        T oldType = fieldIndex.getType();
        T newType = types.get(field);
        if (!oldType.equals(newType)) {
          throw new NotIntegrityException(
              "field "
                  + field
                  + " type is not match, old type: "
                  + oldType
                  + ", new type: "
                  + newType);
        }
        Range<K> range = entry.getValue();
        fieldIndex.addTable(name, range);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeTable(String name) {
    lock.readLock().lock();
    try {
      for (FieldIndex<K, F, T, V> fieldIndex : indexes.values()) {
        fieldIndex.removeTable(name);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(AreaSet<K, F> areas) {
    lock.writeLock().lock();
    try {
      indexes.keySet().removeAll(areas.getFields());
      for (FieldIndex<K, F, T, V> fieldIndex : indexes.values()) {
        fieldIndex.delete(areas.getKeys());
      }
      for (Map.Entry<F, RangeSet<K>> entry : areas.getSegments().entrySet()) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(entry.getKey());
        if (fieldIndex != null) {
          fieldIndex.delete(entry.getValue());
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      indexes.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    // do nothing
  }

  public static class FieldIndex<K extends Comparable<K>, F, T, V> {
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

    public void removeTable(String name) {
      lock.writeLock().lock();
      try {
        this.tableRange.remove(name);
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
        RangeSet<K> validRanges = ranges.complement();
        Iterator<Map.Entry<String, Range<K>>> iterator = tableRange.entrySet().iterator();
        Map<String, Range<K>> overlap = new HashMap<>();
        while (iterator.hasNext()) {
          Map.Entry<String, Range<K>> entry = iterator.next();
          if (!ranges.intersects(entry.getValue())) {
            continue;
          }
          if (ranges.encloses(entry.getValue())) {
            iterator.remove();
          } else {
            overlap.put(entry.getKey(), validRanges.subRangeSet(entry.getValue()).span());
          }
        }
        tableRange.putAll(overlap);
      } finally {
        lock.writeLock().unlock();
      }
    }

    public RangeSet<K> ranges() {
      TreeRangeSet<K> rangeSet = TreeRangeSet.create();
      lock.readLock().lock();
      try {
        for (Range<K> range : tableRange.values()) {
          rangeSet.add(range);
        }
      } finally {
        lock.readLock().unlock();
      }
      return ImmutableRangeSet.copyOf(rangeSet);
    }
  }
}
