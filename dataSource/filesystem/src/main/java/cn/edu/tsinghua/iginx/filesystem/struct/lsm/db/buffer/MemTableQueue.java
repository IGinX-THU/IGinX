/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.MemTableQueueClearEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.TableAppendEvent;
import it.unimi.dsi.fastutil.longs.LongObjectPair;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class MemTableQueue implements NoexceptAutoCloseable {
  private final ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock();

  private final MemTableConfig config;
  private final BlockingQueue<Long> toFlushIds = new PriorityBlockingQueue<>();
  private final ConcurrentNavigableMap<Long, ArchivedMemTable> archives =
      new ConcurrentSkipListMap<>();
  private final BufferAllocator allocator;
  private final ActiveMemTable active;

  public MemTableQueue(
      String name, MemTableConfig config, Semaphore memTablePermits, BufferAllocator allocator) {
    this.config = Preconditions.checkNotNull(config);
    this.allocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
    this.active = new ActiveMemTable(name, config, this.allocator, memTablePermits);
  }

  public WriteBatch prepare(DataView data) {
    return new WriteBatch(WriteBatches.of(data, allocator, config.isEnableAlignInsert()));
  }

  public void store(WriteBatch data) throws InterruptedException {
    queueLock.writeLock().lock();
    try {
      if (active.isOverloaded()) {
        archive(true);
      }
    } finally {
      queueLock.writeLock().unlock();
    }
    active.store(data);
  }

  private void archive(boolean createNewTable) throws InterruptedException {
    queueLock.writeLock().lock();
    try {
      Map<Long, ArchivedMemTable> tables = active.archive(createNewTable);
      archives.putAll(tables);
      toFlushIds.addAll(tables.keySet());
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  public void flushAll(boolean compact) throws InterruptedException {
    List<Awaitable> waiters = new ArrayList<>();
    queueLock.writeLock().lock();
    try {
      archive(compact);
      archives.values().forEach(archivedMemTable -> waiters.add(archivedMemTable::waitUntilClosed));
    } finally {
      queueLock.writeLock().unlock();
    }
    for (Awaitable waiter : waiters) {
      waiter.await();
    }
  }

  public TookTable takeToFlush() throws InterruptedException {
    long id = toFlushIds.take();
    queueLock.writeLock().lock();
    try {
      ArchivedMemTable archivedMemTable = archives.get(id);
      MemTable.Snapshot snapshot = archivedMemTable.getMemTable().snapshot(allocator);
      return new TookTable(
          id,
          new InMemoryTable(id, snapshot),
          archivedMemTable.isOwnTable(),
          toFlushIds::add,
          this::remove);
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  private void remove(long id) {
    ArchivedMemTable removed = archives.remove(id);
    if (removed != null) {
      removed.close();
    }
    active.onTableFlushed(id);
  }

  public long takeToDelete() throws InterruptedException {
    return active.takeToDelete();
  }

  public List<InMemoryTable> snapshot(
      List<cn.edu.tsinghua.iginx.engine.shared.data.read.Field> iginxFields) {
    List<Field> fields = ArrowFields.of(iginxFields);
    List<InMemoryTable> result = new ArrayList<>();
    queueLock.readLock().lock();
    try {
      for (Map.Entry<Long, ArchivedMemTable> entry : archives.entrySet()) {
        long id = entry.getKey();
        ArchivedMemTable archivedMemTable = entry.getValue();
        MemTable.Snapshot snapshot = archivedMemTable.getMemTable().snapshot(fields, allocator);
        result.add(new InMemoryTable(id, snapshot));
      }
      LongObjectPair<MemTable.Snapshot> activeSnapshot = active.snapshot(fields, allocator);
      if (activeSnapshot != null) {
        long id = activeSnapshot.leftLong();
        MemTable.Snapshot snapshot = activeSnapshot.right();
        result.add(new InMemoryTable(id, snapshot));
      }
    } finally {
      queueLock.readLock().unlock();
    }
    return result;
  }

  public void clear() {
    MemTableQueueClearEvent event = new MemTableQueueClearEvent();
    queueLock.writeLock().lock();
    try {
      event.allocatedMemory = allocator.getAllocatedMemory();
      event.begin();
      toFlushIds.clear();
      archives.values().forEach(ArchivedMemTable::close);
      archives.clear();
      active.clear();
      if (allocator.getAllocatedMemory() > 0) {
        throw new IllegalStateException("allocator is not empty: " + allocator.toVerboseString());
      }
      event.end();
    } finally {
      queueLock.writeLock().unlock();
      event.commit();
    }
  }

  @Override
  public void close() {
    queueLock.writeLock().lock();
    try {
      clear();
      active.close();
      allocator.close();
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  static class ArchivedMemTable implements NoexceptAutoCloseable {
    private final MemTable memTable;
    private final BufferAllocator allocator;
    private final NoexceptAutoCloseable onClose;
    private final boolean ownTable;
    private final CountDownLatch latch = new CountDownLatch(1);

    public ArchivedMemTable(
        @WillCloseWhenClosed MemTable memTable,
        @WillCloseWhenClosed BufferAllocator allocator,
        boolean ownTable,
        @WillCloseWhenClosed NoexceptAutoCloseable onClose) {
      this.memTable = Preconditions.checkNotNull(memTable);
      this.allocator = Preconditions.checkNotNull(allocator);
      this.ownTable = ownTable;
      this.onClose = onClose;
    }

    public MemTable getMemTable() {
      return memTable;
    }

    private boolean isOwnTable() {
      return ownTable;
    }

    public void waitUntilClosed() throws InterruptedException {
      latch.await();
    }

    @Override
    public void close() {
      latch.countDown();
      if (ownTable) {
        memTable.close();
        allocator.close();
      }
      onClose.close();
    }
  }

  static class ActiveMemTable {
    private final ReentrantReadWriteLock switchTableLock = new ReentrantReadWriteLock(true);

    private final String name;
    private final MemTableConfig config;
    private final Semaphore memTablePermits;
    private final BufferAllocator allocator;

    private long currentId = 0;
    private BufferAllocator activeAllocator;
    private MemTable activeTable;
    private volatile boolean activeTableWritten;
    private final NavigableSet<Long> uncommittedIds = new TreeSet<>();
    private final BlockingQueue<Long> toDeleteIds = new PriorityBlockingQueue<>();

    public ActiveMemTable(
        String name, MemTableConfig config, BufferAllocator allocator, Semaphore memTablePermits) {
      this.name = Preconditions.checkNotNull(name);
      this.config = Preconditions.checkNotNull(config);
      this.allocator = Preconditions.checkNotNull(allocator);
      this.memTablePermits = Preconditions.checkNotNull(memTablePermits);
      createNewMemtable();
    }

    private void createNewMemtable() {
      String name =
          String.join(
              "-", allocator.getName(), MemTable.class.getSimpleName(), String.valueOf(currentId));
      activeAllocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
      activeTable = new MemTable(activeAllocator, config.getChunkValues());
      activeTableWritten = false;
    }

    public boolean isOverloaded() {
      switchTableLock.readLock().lock();
      try {
        return activeAllocator.getAllocatedMemory() >= config.getCapacity().toBytes();
      } finally {
        switchTableLock.readLock().unlock();
      }
    }

    public void store(Iterable<MemBatch.Snapshot> data) {
      TableAppendEvent event = new TableAppendEvent();
      switchTableLock.readLock().lock();
      try {
        event.activeMemTableName = name;
        event.memTableId = currentId;
        event.begin();
        for (MemBatch.Snapshot snapshot : data) {
          activeTable.append(snapshot);
        }
        event.end();
        activeTableWritten = true;
      } finally {
        switchTableLock.readLock().unlock();
        event.commit();
      }
    }

    public Map<Long, ArchivedMemTable> archive(boolean createNewTable) throws InterruptedException {
      switchTableLock.writeLock().lock();
      try {
        if (!activeTableWritten) {
          return Collections.emptyMap();
        }
        memTablePermits.acquire();

        Map<Long, ArchivedMemTable> result = new HashMap<>();
        long archiveId = currentId++;
        result.put(
            archiveId,
            new ArchivedMemTable(
                activeTable, activeAllocator, createNewTable, memTablePermits::release));

        if (createNewTable) {
          createNewMemtable();
        } else {
          uncommittedIds.add(archiveId);
        }
        return result;
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    @Nullable
    public LongObjectPair<MemTable.Snapshot> snapshot(
        List<Field> fields, BufferAllocator allocator) {
      switchTableLock.readLock().lock();
      try {
        if (activeTableWritten) {
          return LongObjectPair.of(currentId, activeTable.snapshot(fields, allocator));
        }
        return null;
      } finally {
        switchTableLock.readLock().unlock();
      }
    }

    public long takeToDelete() throws InterruptedException {
      return toDeleteIds.take();
    }

    public void onTableFlushed(long id) {
      switchTableLock.writeLock().lock();
      try {
        Set<Long> toDeleteIds = uncommittedIds.subSet(Long.MIN_VALUE, id);
        this.toDeleteIds.addAll(toDeleteIds);
        toDeleteIds.clear();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    public void clear() {
      switchTableLock.writeLock().lock();
      try {
        activeTable.close();
        activeAllocator.close();
        currentId = 0;
        createNewMemtable();
        uncommittedIds.clear();
        toDeleteIds.clear();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    public void close() {
      switchTableLock.writeLock().lock();
      try {
        clear();
        activeTable.close();
        activeAllocator.close();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }
  }

  public static class TookTable implements NoexceptAutoCloseable {

    private final long id;
    private final InMemoryTable table;
    private final boolean finalTable;
    private final LongConsumer onFailure;
    private final LongConsumer onSuccess;
    private boolean failed = false;

    TookTable(
        long id,
        @WillCloseWhenClosed InMemoryTable memTable,
        boolean finalTable,
        LongConsumer onFailure,
        LongConsumer onSuccess) {
      this.id = id;
      this.table = memTable;
      this.finalTable = finalTable;
      this.onFailure = onFailure;
      this.onSuccess = onSuccess;
    }

    public long getId() {
      return id;
    }

    public Table getMemTable() {
      return table;
    }

    public void fail() {
      failed = true;
    }

    @Override
    public void close() {
      table.close();
      if (failed) {
        onFailure.accept(id);
      } else {
        onSuccess.accept(id);
      }
    }

    public boolean isFinalTable() {
      return finalTable;
    }
  }
}
