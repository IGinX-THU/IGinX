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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
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

public class TableIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableIndex.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final Map<String, FieldIndex> indexes = new HashMap<>();

  public TableIndex(TableStorage tableStorage) {
    try {
      for (String tableName : tableStorage.reload()) {
        LOGGER.debug("rebuilt table index for table: {}", tableName);
        TableMeta meta = tableStorage.getMeta(tableName);
        declareFields(meta.getSchema());
        addTable(tableName, meta);
      }
    } catch (IOException | TypeConflictedException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public Set<String> find(AreaSet<Long, String> areas) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      RangeSet<Long> rangeSet = areas.getKeys();
      if (!rangeSet.isEmpty()) {
        for (FieldIndex fieldIndex : indexes.values()) {
          Set<String> tables = fieldIndex.find(rangeSet);
          result.addAll(tables);
        }
      }
      for (String field : areas.getFields()) {
        FieldIndex fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          continue;
        }
        Set<String> tables = fieldIndex.find();
        result.addAll(tables);
      }
      for (Map.Entry<String, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        FieldIndex fieldIndex = indexes.get(entry.getKey());
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

  public Map<String, Range<Long>> ranges() {
    Map<String, Range<Long>> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<String, FieldIndex> entry : indexes.entrySet()) {
        RangeSet<Long> rangeSet = entry.getValue().ranges();
        if (!rangeSet.isEmpty()) {
          result.put(entry.getKey(), rangeSet.span());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void declareFields(Map<String, DataType> schema) throws TypeConflictedException {
    boolean hasNewField = checkOldFields(schema);
    if (hasNewField) {
      declareNewFields(schema);
    }
  }

  private boolean checkOldFields(Map<String, DataType> schema) throws TypeConflictedException {
    boolean hasNewField = false;
    lock.readLock().lock();
    try {
      for (Map.Entry<String, DataType> entry : schema.entrySet()) {
        FieldIndex fieldIndex = indexes.get(entry.getKey());
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

  private void declareNewFields(Map<String, DataType> schema) throws TypeConflictedException {
    Map<String, DataType> newSchema = new HashMap<>();
    lock.writeLock().lock();
    try {
      for (Map.Entry<String, DataType> entry : schema.entrySet()) {
        FieldIndex fieldIndex = indexes.get(entry.getKey());
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
      for (Map.Entry<String, DataType> entry : newSchema.entrySet()) {
        indexes.put(entry.getKey(), new FieldIndex(entry.getValue()));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<String, DataType> getType(Set<String> fields, Consumer<String> processMissingField) {
    Map<String, DataType> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (String field : fields) {
        FieldIndex fieldIndex = indexes.get(field);
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

  public Map<String, DataType> getType() {
    Map<String, DataType> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<String, FieldIndex> entry : indexes.entrySet()) {
        result.put(entry.getKey(), entry.getValue().getType());
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void addTable(String name, TableMeta meta) {
    lock.readLock().lock();
    try {
      Map<String, DataType> types = meta.getSchema();
      for (String field : types.keySet()) {
        FieldIndex fieldIndex = indexes.get(field);
        if (fieldIndex == null) {
          throw new NotIntegrityException("field " + field + " is not found in schema");
        }
        DataType oldType = fieldIndex.getType();
        DataType newType = types.get(field);
        if (!oldType.equals(newType)) {
          throw new NotIntegrityException(
              "field "
                  + field
                  + " type is not match, old type: "
                  + oldType
                  + ", new type: "
                  + newType);
        }
        Range<Long> range = meta.getRange(field);
        fieldIndex.addTable(name, Objects.requireNonNull(range));
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeTable(String name) {
    lock.readLock().lock();
    try {
      for (FieldIndex fieldIndex : indexes.values()) {
        fieldIndex.removeTable(name);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(AreaSet<Long, String> areas) {
    lock.writeLock().lock();
    try {
      indexes.keySet().removeAll(areas.getFields());
      for (FieldIndex fieldIndex : indexes.values()) {
        fieldIndex.delete(areas.getKeys());
      }
      for (Map.Entry<String, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        FieldIndex fieldIndex = indexes.get(entry.getKey());
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

  public static class FieldIndex {
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final DataType type;
    private final Map<String, Range<Long>> tableRange = new HashMap<>();

    public FieldIndex(DataType type) {
      this.type = type;
    }

    public DataType getType() {
      return type;
    }

    public void addTable(String name, Range<Long> range) {
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

    public Set<String> find(RangeSet<Long> ranges) {
      Set<String> result = new HashSet<>();
      lock.readLock().lock();
      try {
        for (Map.Entry<String, Range<Long>> entry : tableRange.entrySet()) {
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

    public void delete(RangeSet<Long> ranges) {
      lock.writeLock().lock();
      try {
        RangeSet<Long> validRanges = ranges.complement();
        Iterator<Map.Entry<String, Range<Long>>> iterator = tableRange.entrySet().iterator();
        Map<String, Range<Long>> overlap = new HashMap<>();
        while (iterator.hasNext()) {
          Map.Entry<String, Range<Long>> entry = iterator.next();
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

    public RangeSet<Long> ranges() {
      TreeRangeSet<Long> rangeSet = TreeRangeSet.create();
      lock.readLock().lock();
      try {
        for (Range<Long> range : tableRange.values()) {
          rangeSet.add(range);
        }
      } finally {
        lock.readLock().unlock();
      }
      return ImmutableRangeSet.copyOf(rangeSet);
    }
  }
}
