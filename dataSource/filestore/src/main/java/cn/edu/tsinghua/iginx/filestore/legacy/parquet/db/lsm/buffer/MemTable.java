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
package cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.lsm.buffer.chunk.IndexedChunk;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.arrow.ArrowFields;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class MemTable implements AutoCloseable {

  private final ConcurrentHashMap<Field, MemColumn> columns = new ConcurrentHashMap<>();
  private final IndexedChunk.Factory factory;
  private final BufferAllocator allocator;
  private final int maxChunkValueCount;
  private final int minChunkValueCount;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile boolean closed = false;

  public MemTable(
      IndexedChunk.Factory factory,
      BufferAllocator allocator,
      int maxChunkValueCount,
      int minChunkValueCount) {
    this.factory = factory;
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
    this.minChunkValueCount = minChunkValueCount;
  }

  public void reset() {
    lock.writeLock().lock();
    try {
      columns.values().forEach(MemColumn::close);
      columns.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    closed = true;
    reset();
  }

  public Set<Field> getFields() {
    return Collections.unmodifiableSet(columns.keySet());
  }

  public MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumn.Snapshot> columns = new LinkedHashMap<>();
    lock.readLock().lock();
    try {
      for (Field field : fields) {
        if (columns.containsKey(field)) {
          throw new IllegalArgumentException("Duplicate field: " + field);
        }
        MemColumn column = this.columns.get(field);
        if (column != null) {
          columns.put(field, column.snapshot(ranges, allocator));
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return new MemoryTable(columns);
  }

  public MemoryTable snapshot(BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumn.Snapshot> columns = new LinkedHashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<Field, MemColumn> entry : this.columns.entrySet()) {
        columns.put(entry.getKey(), entry.getValue().snapshot(allocator));
      }
    } finally {
      lock.readLock().unlock();
    }
    return new MemoryTable(columns);
  }

  public void store(Iterable<Chunk.Snapshot> data) {
    lock.readLock().lock();
    try {
      data.forEach(this::store);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void store(Chunk.Snapshot data) {
    lock.readLock().lock();
    try {
      if (closed) {
        throw new IllegalStateException("MemTable is closed");
      }
      Field field = ArrowFields.nullable(data.getField());
      MemColumn column =
          columns.computeIfAbsent(
              field,
              key -> new MemColumn(factory, allocator, maxChunkValueCount, minChunkValueCount));
      column.store(data);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void compact() {
    lock.readLock().lock();
    try {
      columns.values().forEach(MemColumn::compact);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(AreaSet<Long, Field> areas) {
    lock.writeLock().lock();
    try {
      for (Field field : areas.getFields()) {
        MemColumn column = columns.remove(field);
        if (column != null) {
          column.close();
        }
      }
      lock.readLock().lock();
    } finally {
      lock.writeLock().unlock();
    }
    try {
      RangeSet<Long> keys = areas.getKeys();
      if (!keys.isEmpty()) {
        columns.values().forEach(column -> column.delete(keys));
      }
      for (Map.Entry<Field, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        Field field = entry.getKey();
        RangeSet<Long> ranges = entry.getValue();
        MemColumn column = columns.get(field);
        if (column != null) {
          column.delete(ranges);
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }
}
