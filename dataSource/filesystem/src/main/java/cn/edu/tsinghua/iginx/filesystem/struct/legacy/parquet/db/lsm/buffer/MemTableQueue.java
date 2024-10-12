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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Awaitable;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.arrow.ArrowFields;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import javax.annotation.Nonnegative;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class MemTableQueue implements NoexceptAutoCloseable {
  private final Logger LOGGER = LoggerFactory.getLogger(MemTableQueue.class);

  private final ReentrantLock checkSizeLock = new ReentrantLock(true);
  private final ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock(true);
  private final ReentrantLock pollLock = new ReentrantLock(true);
  private final Condition pollLockCond = pollLock.newCondition();

  private final NavigableMap<Long, ArchivedMemTable> archives = new TreeMap<>();
  private final BufferAllocator allocator;
  private final ActiveMemTable active;

  public MemTableQueue(Shared shared, BufferAllocator allocator) {
    String allocatorName =
        String.join("-", allocator.getName(), MemTableQueue.class.getSimpleName());
    this.allocator = allocator.newChildAllocator(allocatorName, 0, Long.MAX_VALUE);
    this.active = new ActiveMemTable(shared, this.allocator);
  }

  public void store(Iterable<Chunk.Snapshot> data) throws InterruptedException {
    checkSizeLock.lock();
    try {
      if (active.isOverloaded()) {
        compact();
      }
    } finally {
      checkSizeLock.unlock();
    }
    active.store(data);
  }

  public void compact() throws InterruptedException {
    checkSizeLock.lock();
    try {
      Map<Long, ArchivedMemTable> temp = active.archive();
      queueLock.writeLock().lock();
      try {
        archives.putAll(temp);
        temp.clear();
      } finally {
        queueLock.writeLock().unlock();
        temp.values().forEach(ArchivedMemTable::close);
      }
    } finally {
      checkSizeLock.unlock();
    }
    signalAll();
  }

  public void flush() throws InterruptedException {
    Awaitable flush;
    List<Awaitable> waiters = new ArrayList<>();
    queueLock.readLock().lock();
    try {
      flush = active.flush();
      for (ArchivedMemTable archivedMemTable : archives.values()) {
        waiters.add(archivedMemTable::waitUntilClosed);
      }
    } finally {
      queueLock.readLock().unlock();
    }
    signalAll();
    flush.await();
    for (Awaitable waiter : waiters) {
      waiter.await();
    }
  }

  public void delete(AreaSet<Long, Field> areas) {
    queueLock.readLock().lock();
    try {
      active.delete(areas);
      archives.values().forEach(archived -> archived.delete(areas));
    } finally {
      queueLock.readLock().unlock();
    }
  }

  private void await() throws InterruptedException {
    pollLock.lock();
    try {
      pollLockCond.await();
    } finally {
      pollLock.unlock();
    }
  }

  private void signalAll() {
    pollLock.lock();
    try {
      pollLockCond.signalAll();
    } finally {
      pollLock.unlock();
    }
  }

  /**
   * take next table id, if no table id available, block until new table id available
   *
   * @param idAtLeast the non-negative table id at least
   * @return the next non-negative table id
   */
  @Nonnegative
  public long awaitNext(@Nonnegative long idAtLeast) throws InterruptedException {
    while (true) {
      queueLock.readLock().lock();
      try {
        Long nextId = archives.ceilingKey(idAtLeast);
        if (nextId != null) {
          return nextId;
        }
        Long activeNextId = active.newestKey(idAtLeast);
        if (activeNextId != null) {
          return activeNextId;
        }
      } finally {
        queueLock.readLock().unlock();
      }
      await();
    }
  }

  public void eliminate(long id, Consumer<AreaSet<Long, Field>> commiter)
      throws InterruptedException {
    active.eliminate(id);

    queueLock.writeLock().lock();
    try {
      if (archives.containsKey(id)) {
        try (ArchivedMemTable memTable = archives.remove(id)) {
          commiter.accept(memTable.getDeleted());
        }
        return;
      }
    } finally {
      queueLock.writeLock().unlock();
    }

    commiter.accept(AreaSet.all());
  }

  public MemoryTable snapshot(long id, BufferAllocator allocator) {
    queueLock.readLock().lock();
    try {
      ArchivedMemTable archived = archives.get(id);
      if (archived != null) {
        return archived.snapshot(allocator);
      }
      return active.snapshot(id, allocator);
    } finally {
      queueLock.readLock().unlock();
    }
  }

  public List<Scanner<Long, Scanner<String, Object>>> scan(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) throws IOException {
    Set<String> innerFields = ArrowFields.toInnerFields(fields);
    List<Scanner<Long, Scanner<String, Object>>> scanners = new ArrayList<>();
    queueLock.readLock().lock();
    try {
      for (ArchivedMemTable archivedMemTable : archives.values()) {
        try (MemoryTable table = archivedMemTable.snapshot(fields, ranges, allocator)) {
          Scanner<Long, Scanner<String, Object>> scanner = table.scan(innerFields, ranges);
          AreaSet<Long, Field> deleted = archivedMemTable.getDeleted();
          if (deleted.isEmpty()) {
            scanners.add(scanner);
          } else {
            AreaSet<Long, String> innerDelete = ArrowFields.toInnerAreas(deleted);
            scanners.add(new AreaFilterScanner<>(scanner, innerDelete));
          }
        }
      }
      active.scan(fields, ranges, allocator, scanners::add);
    } catch (Exception e) {
      try {
        AutoCloseables.close(scanners);
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw e;
    } finally {
      queueLock.readLock().unlock();
    }
    return scanners;
  }

  public void clear() {
    queueLock.writeLock().lock();
    try {
      active.reset();
      archives.values().forEach(ArchivedMemTable::close);
      archives.clear();
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    clear();
    allocator.close();
  }
}
