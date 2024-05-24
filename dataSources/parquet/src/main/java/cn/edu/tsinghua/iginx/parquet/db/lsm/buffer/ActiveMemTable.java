package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Awaitable;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

public class ActiveMemTable {

  private final ReentrantReadWriteLock switchTableLock = new ReentrantReadWriteLock(true);
  private final ReentrantLock createLock = new ReentrantLock(true);
  private final ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock(true);

  private final Shared shared;
  private final BufferAllocator allocator;

  private long activeId = 0;
  private BufferAllocator activeAllocator = null;
  private MemTable activeTable = null;
  private final NavigableMap<Long, CountDownLatch> awaiting = new TreeMap<>();

  ActiveMemTable(Shared shared, BufferAllocator allocator) {
    this.shared = Preconditions.checkNotNull(shared);
    this.allocator = Preconditions.checkNotNull(allocator);
  }

  public boolean isOverloaded() {
    switchTableLock.readLock().lock();
    try {
      return activeAllocator != null
          && activeAllocator.getAllocatedMemory()
              >= shared.getStorageProperties().getWriteBufferSize();
    } finally {
      switchTableLock.readLock().unlock();
    }
  }

  public void store(Iterable<Chunk.Snapshot> data) {
    switchTableLock.readLock().lock();
    try {
      createMemtableIfNotExist();
      activeTable.store(data);
    } finally {
      switchTableLock.readLock().unlock();
    }
  }

  private void createMemtableIfNotExist() {
    createLock.lock();
    try {
      if (activeTable == null) {
        assert activeAllocator == null;
        String name =
            String.join(
                "-", allocator.getName(), MemTable.class.getSimpleName(), String.valueOf(activeId));
        activeAllocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
        activeTable =
            new MemTable(
                shared.getStorageProperties().getWriteBufferChunkFactory(),
                activeAllocator,
                shared.getStorageProperties().getWriteBufferChunkValuesMax(),
                shared.getStorageProperties().getWriteBufferChunkValuesMin());
      }
    } finally {
      createLock.unlock();
    }
  }

  public Awaitable flush() {
    flushLock.writeLock().lock();
    try {
      switchTableLock.readLock().lock();
      try {
        if (activeTable == null) {
          return () -> {};
        }
      } finally {
        switchTableLock.readLock().unlock();
      }
      CountDownLatch latch = new CountDownLatch(1);
      awaiting.put(activeId++, latch);
      return latch::await;
    } finally {
      flushLock.writeLock().unlock();
    }
  }

  public Map<Long, ArchivedMemTable> archive() throws InterruptedException {
    shared.getMemTablePermits().acquire();
    flushLock.writeLock().lock();
    switchTableLock.writeLock().lock();
    try {
      if (activeTable == null) {
        shared.getMemTablePermits().release();
        return Collections.emptyMap();
      }
      Map<Long, ArchivedMemTable> result = new HashMap<>();
      List<NoexceptAutoCloseable> onClose = new ArrayList<>();
      awaiting.values().forEach(latch -> onClose.add(latch::countDown));
      onClose.add(activeAllocator::close);
      onClose.add(() -> shared.getMemTablePermits().release());
      result.put(activeId++, new ArchivedMemTable(activeTable, onClose));
      awaiting.clear();
      activeTable = null;
      activeAllocator = null;
      return result;
    } finally {
      switchTableLock.writeLock().unlock();
      flushLock.writeLock().unlock();
    }
  }

  public MemoryTable snapshot(long id, BufferAllocator allocator) {
    flushLock.readLock().lock();
    try {
      if (awaiting.containsKey(id)) {
        switchTableLock.readLock().lock();
        try {
          return activeTable.snapshot(allocator);
        } finally {
          switchTableLock.readLock().unlock();
        }
      } else {
        return MemoryTable.empty();
      }
    } finally {
      flushLock.readLock().unlock();
    }
  }

  public Long newestKey(long idAtLeast) {
    flushLock.readLock().lock();
    try {
      if (!awaiting.isEmpty()) {
        long id = awaiting.lastKey();
        if (id >= idAtLeast) {
          return id;
        }
      }
      return null;
    } finally {
      flushLock.readLock().unlock();
    }
  }

  public void eliminate(long id) throws InterruptedException {
    CountDownLatch latch = null;
    flushLock.writeLock().lock();
    try {
      if (awaiting.containsKey(id)) {
        latch = new CountDownLatch(1);
      }
      SortedMap<Long, CountDownLatch> older = awaiting.headMap(id, true);
      older.values().forEach(CountDownLatch::countDown);
      older.clear();
      if (latch != null) {
        awaiting.put(id, latch);
      }
    } finally {
      flushLock.writeLock().unlock();
    }
    if (latch != null) {
      latch.await();
    }
  }

  public void delete(AreaSet<Long, Field> areas) {
    switchTableLock.readLock().lock();
    try {
      if (activeTable != null) {
        activeTable.delete(areas);
      }
    } finally {
      switchTableLock.readLock().unlock();
    }
  }

  public void reset() {
    flushLock.writeLock().lock();
    switchTableLock.writeLock().lock();
    try {
      if (activeTable != null) {
        activeTable.close();
        activeAllocator.close();
      }
      activeTable = null;
      activeAllocator = null;
      awaiting.values().forEach(CountDownLatch::countDown);
      awaiting.clear();
    } finally {
      switchTableLock.writeLock().unlock();
      flushLock.writeLock().unlock();
    }
  }

  public void scan(
      List<Field> fields,
      RangeSet<Long> ranges,
      BufferAllocator allocator,
      Consumer<Scanner<Long, Scanner<String, Object>>> consumer)
      throws IOException {
    switchTableLock.readLock().lock();
    try {
      if (activeTable != null) {
        Set<String> innerFields = ArrowFields.toInnerFields(fields);
        try (MemoryTable table = activeTable.snapshot(fields, ranges, allocator)) {
          consumer.accept(table.scan(innerFields, ranges));
        }
      }
    } finally {
      switchTableLock.readLock().unlock();
    }
  }
}
