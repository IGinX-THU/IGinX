package cn.edu.tsinghua.iginx.parquet.db.lsm.index;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.Tombstone;
import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.TombstoneStorage;
import cn.edu.tsinghua.iginx.parquet.shared.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.shared.exception.TypeConflictedException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
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

  public TableIndex(
      TableStorage<K, F, T, V> tableStorage, TombstoneStorage<K, F> tombstoneStorage) {
    try {
      for (String tableName : tableStorage.tableNames()) {
        TableMeta<K, F, T, V> meta = tableStorage.get(tableName).getMeta();
        declareFields(meta.getSchema());
        addTable(tableName, meta);
        Tombstone<K, F> tombstone = tombstoneStorage.get(tableName);
        delete(tombstone.getDeletedColumns());
        delete(tombstone.getDeletedRows());
        for (Map.Entry<F, RangeSet<K>> entry : tombstone.getDeletedRanges().entrySet()) {
          delete(Collections.singleton(entry.getKey()), entry.getValue());
        }
      }
    } catch (IOException | TypeConflictedException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public Set<String> find(Set<F> fields, RangeSet<K> ranges) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (F field : fields) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          continue;
        }
        Set<String> tables = fieldIndex.find(ranges);
        result.addAll(tables);
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public Set<String> find(Set<F> fields) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (F field : fields) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          continue;
        }
        Set<String> tables = fieldIndex.find();
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

  public Set<String> find(RangeSet<K> ranges) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (FieldIndex<K, F, T, V> fieldIndex : indexes.values()) {
        Set<String> tables = fieldIndex.find(ranges);
        result.addAll(tables);
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

  public void delete(Set<F> fields, RangeSet<K> ranges) {
    lock.readLock().lock();
    try {
      for (F field : fields) {
        FieldIndex<K, F, T, V> fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          continue;
        }
        fieldIndex.delete(ranges);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(Set<F> fields) {
    lock.writeLock().lock();
    try {
      indexes.keySet().removeAll(fields);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void delete(RangeSet<K> ranges) {
    lock.readLock().lock();
    try {
      for (FieldIndex<K, F, T, V> fieldIndex : indexes.values()) {
        fieldIndex.delete(ranges);
      }
    } finally {
      lock.readLock().unlock();
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
}
