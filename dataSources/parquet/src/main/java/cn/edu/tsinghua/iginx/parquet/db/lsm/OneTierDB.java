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
package cn.edu.tsinghua.iginx.parquet.db.lsm;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableIndex;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
  private final Shared shared;
  private final TableStorage<K, F, T, V> tableStorage;
  private final TableIndex<K, F, T, V> tableIndex;

  private DataBuffer<K, F, V> writeBuffer = new DataBuffer<>();
  private String previousTableName = null;
  private final AtomicLong bufferDirtiedTime = new AtomicLong(Long.MAX_VALUE);
  private final LongAdder bufferInsertedSize = new LongAdder();
  private final Lock checkLock = new ReentrantLock(true);
  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock commitLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock storageLock = new ReentrantReadWriteLock(true);

  private final ScheduledExecutorService scheduler;
  private final String name;
  private final long timeout;

  public OneTierDB(String name, Shared shared, ReadWriter<K, F, T, V> readerWriter)
      throws IOException {
    this.name = name;
    this.shared = shared;
    this.tableStorage = new TableStorage<>(shared, readerWriter);
    this.tableIndex = new TableIndex<>(tableStorage);

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("OneTierDB-" + name + "-%d").build();
    this.timeout = shared.getStorageProperties().getWriteBufferTimeout().toMillis();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    if (timeout > 0) {
      LOGGER.info("db {} start to check buffer timeout check every {}ms", name, timeout);
      this.scheduler.scheduleWithFixedDelay(
          this::checkBufferTimeout, timeout, timeout, TimeUnit.MILLISECONDS);
    } else {
      LOGGER.info("db {} buffer timeout check is immediately after writing", name);
    }
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      AreaSet<K, F> areas = new AreaSet<>();
      areas.add(fields, ranges);
      Set<String> tables = tableIndex.find(areas);
      List<String> sortedTableNames = new ArrayList<>(tables);
      sortedTableNames.sort(Comparator.naturalOrder());
      for (String tableName : sortedTableNames) {
        Table<K, F, T, V> table = tableStorage.get(tableName);
        try (Scanner<K, Scanner<F, V>> scanner = table.scan(fields, ranges)) {
          readBuffer.putRows(scanner);
        }
      }

      for (Range<K> range : ranges.asRanges()) {
        readBuffer.putRows(writeBuffer.scanRows(fields, range));
      }
    } catch (IOException | StorageException e) {
      throw new RuntimeException(e);
    } finally {
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }

    return readBuffer.scanRows(fields, Range.all());
  }

  @Override
  public Optional<Range<K>> range() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      TreeRangeSet<K> rangeSet = TreeRangeSet.create();
      for (Range<K> range : tableIndex.ranges().values()) {
        rangeSet.add(range);
      }
      for (Range<K> range : writeBuffer.ranges().values()) {
        rangeSet.add(range);
      }
      if (rangeSet.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(rangeSet.span());
      }
    } finally {
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public Map<F, T> schema() throws StorageException {
    return tableIndex.getType();
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        beforeWriting();
        try {
          tableIndex.declareFields(schema);
          try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
            writeBuffer.putRows(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        } finally {
          afterWriting();
        }
      }
    }
    updateDirty();
    if (timeout <= 0) {
      checkBufferTimeout();
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        beforeWriting();
        try {
          tableIndex.declareFields(schema);
          try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
            writeBuffer.putColumns(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        } finally {
          afterWriting();
        }
      }
    }
    updateDirty();
    if (timeout <= 0) {
      checkBufferTimeout();
    }
  }

  private void updateDirty() {
    long currentTime = System.currentTimeMillis();
    bufferDirtiedTime.updateAndGet(oldTime -> Math.min(oldTime, currentTime));
  }

  private void beforeWriting() {
    checkBufferSize();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
  }

  private void afterWriting() {
    storageLock.readLock().unlock();
    deleteLock.readLock().unlock();
    commitLock.readLock().unlock();
  }

  private void checkBufferSize() {
    checkLock.lock();
    try {
      if (bufferInsertedSize.sum() < shared.getStorageProperties().getWriteBufferSize()) {
        return;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "flushing is triggered when write buffer size {} reaching {}",
            bufferInsertedSize.sum(),
            shared.getStorageProperties().getWriteBufferSize());
      }
      commitMemoryTable(false, new CountDownLatch(1));
    } finally {
      checkLock.unlock();
    }
  }

  private void checkBufferTimeout() {
    CountDownLatch latch = new CountDownLatch(1);
    checkLock.lock();
    try {
      long interval = System.currentTimeMillis() - bufferDirtiedTime.get();
      if (interval < timeout) {
        return;
      }

      LOGGER.debug(
          "flushing is triggered when write buffer dirtied time {}ms reaching {}ms",
          interval,
          timeout);
      boolean temp = bufferInsertedSize.sum() < shared.getStorageProperties().getWriteBufferSize();
      commitMemoryTable(temp, latch);
    } finally {
      checkLock.unlock();
    }
    LOGGER.debug("waiting for flushing table");
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    }
    LOGGER.debug("table is flushed");
  }

  private synchronized void commitMemoryTable(boolean temp, CountDownLatch latch) {
    commitLock.writeLock().lock();
    deleteLock.readLock().lock();
    try {
      Map<F, T> types =
          tableIndex.getType(
              writeBuffer.fields(),
              f -> {
                throw new NotIntegrityException("field " + f + " is not found in schema");
              });

      MemoryTable<K, F, T, V> table = new MemoryTable<>(writeBuffer, types);

      String toDelete = previousTableName;
      String committedTableName =
          tableStorage.flush(
              table,
              () -> {
                if (toDelete != null) {
                  LOGGER.debug("delete table {}", toDelete);
                  storageLock.writeLock().lock();
                  try {
                    tableIndex.removeTable(toDelete);
                    tableStorage.remove(toDelete);
                  } catch (Throwable e) {
                    LOGGER.error("failed to delete table {}", toDelete, e);
                  } finally {
                    storageLock.writeLock().unlock();
                  }
                }
                latch.countDown();
              });

      LOGGER.debug(
          "submit table {} to flush, with temp={}, latch={}", committedTableName, temp, latch);
      tableIndex.addTable(committedTableName, table.getMeta());
      this.bufferDirtiedTime.set(Long.MAX_VALUE);
      previousTableName = committedTableName;
      if (!temp) {
        LOGGER.info("reset write buffer");
        this.writeBuffer = new DataBuffer<>();
        bufferInsertedSize.reset();
        previousTableName = null;
      }
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.readLock().unlock();
      commitLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(AreaSet<K, F> areas) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    storageLock.readLock().lock();
    try {
      LOGGER.info("start to delete {} in {}", areas, name);
      writeBuffer.remove(areas);
      Set<String> tables = tableIndex.find(areas);
      tableStorage.delete(tables, areas);
      tableIndex.delete(areas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      storageLock.readLock().unlock();
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void clear() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", name);
      previousTableName = null;
      tableStorage.clear();
      tableIndex.clear();
      writeBuffer.clear();
      bufferDirtiedTime.set(Long.MAX_VALUE);
      bufferInsertedSize.reset();
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    if (shared.getStorageProperties().toFlushOnClose()) {
      LOGGER.info("flushing is triggered when closing");
      CountDownLatch latch = new CountDownLatch(1);
      commitMemoryTable(false, latch);
      latch.await();
    }
    tableStorage.close();
    tableIndex.close();
  }
}
