/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

  public OneTierDB(Shared shared, Path dir, ReadWriter<K, F, T, V> readerWriter)
      throws IOException {
    this.shared = shared;
    this.tableStorage = new TableStorage<>(shared, readerWriter);
    this.tableIndex = new TableIndex<>(tableStorage);

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("OneTierDB-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    long interval = shared.getStorageProperties().getWriteBufferTimeout().toMillis();
    this.scheduler.scheduleAtFixedRate(
        this::scheduleTask, interval, interval, TimeUnit.MILLISECONDS);
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
      ImmutableRangeSet.Builder<K> builder = ImmutableRangeSet.builder();
      for (Range<K> range : tableIndex.ranges().values()) {
        builder.add(range);
      }
      for (Range<K> range : writeBuffer.ranges().values()) {
        builder.add(range);
      }
      ImmutableRangeSet<K> rangeSet = builder.build();
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
    beforeWriting();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
            writeBuffer.putRows(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        }
      }
    } finally {
      updateDirty();
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    beforeWriting();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
            writeBuffer.putColumns(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        }
      }
    } finally {
      updateDirty();
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  private void updateDirty() {
    long currentTime = System.currentTimeMillis();
    bufferDirtiedTime.updateAndGet(oldTime -> Math.min(oldTime, currentTime));
  }

  private void beforeWriting() {
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
      commitMemoryTable(true);
    } finally {
      checkLock.unlock();
    }
  }

  private void scheduleTask() {
    checkLock.lock();
    try {
      long interval = System.currentTimeMillis() - bufferDirtiedTime.get();
      if (interval < shared.getStorageProperties().getWriteBufferTimeout().toMillis()) {
        return;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "flushing is triggered when write buffer dirtied time {} reaching {}",
            interval,
            shared.getStorageProperties().getWriteBufferTimeout());
      }
      commitMemoryTable(false);
    } finally {
      checkLock.unlock();
    }
  }

  private synchronized void commitMemoryTable(boolean switchBuffer) {
    commitLock.writeLock().lock();
    deleteLock.readLock().lock();
    try {
      Map<F, T> types =
          tableIndex.getType(
              writeBuffer.fields(),
              f -> {
                throw new NotIntegrityException("field " + f + " is not found in schema");
              });

      Runnable afterFlush = () -> {};
      if (previousTableName != null) {
        String toDelete = previousTableName;
        afterFlush =
            () -> {
              storageLock.writeLock().lock();
              try {
                tableIndex.removeTable(toDelete);
                tableStorage.remove(toDelete);
              } catch (Throwable e) {
                LOGGER.error("failed to delete table {}", toDelete, e);
              } finally {
                storageLock.writeLock().unlock();
              }
            };
      }

      MemoryTable<K, F, T, V> table = new MemoryTable<>(writeBuffer, types);
      String committedTableName = tableStorage.flush(table, afterFlush);
      tableIndex.addTable(committedTableName, table.getMeta());
      this.bufferDirtiedTime.set(Long.MAX_VALUE);
      previousTableName = committedTableName;
      if (switchBuffer) {
        this.writeBuffer = new DataBuffer<>();
        bufferInsertedSize.reset();
        previousTableName = null;
      }
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
    storageLock.writeLock().lock();
    try {
      tableStorage.clear();
      tableIndex.clear();
      writeBuffer.clear();
      bufferDirtiedTime.set(Long.MAX_VALUE);
      bufferInsertedSize.reset();
    } finally {
      storageLock.writeLock().unlock();
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("flushing is triggered when closing");
    scheduler.shutdown();
    commitMemoryTable(true);
    tableStorage.close();
    tableIndex.close();
  }
}
