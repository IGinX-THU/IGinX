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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.MemTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.UnorderedChunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableIndex;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.WriteBatches;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.parquet.util.CloseableHolders;
import cn.edu.tsinghua.iginx.parquet.util.CloseableHolders.NoexceptAutoCloseableHolder;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.WillClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final Lock checkLock = new ReentrantLock(true);
  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock commitLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock storageLock = new ReentrantReadWriteLock(true);
  private final String name;
  private final long timeout;
  private final Shared shared;

  private final ScheduledExecutorService scheduler;
  private final BufferAllocator allocator;
  private final TableStorage<K, F, T, V> tableStorage;
  private final TableIndex<K, F, T, V> tableIndex;

  private MemTable memtable;
  private String previousTableName = null;

  public OneTierDB(String name, Shared shared, ReadWriter<K, F, T, V> readerWriter)
      throws IOException {
    this.name = name;
    this.shared = shared;
    this.allocator = shared.getAllocator().newChildAllocator(name, 0, Long.MAX_VALUE);
    this.tableStorage = new TableStorage<>(shared, readerWriter);
    this.tableIndex = new TableIndex<>(tableStorage);
    this.memtable = nextMemTable();

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

  private MemTable nextMemTable() {
    return new MemTable(
        tableStorage.nextTableId(),
        shared.getStorageProperties().getWriteBufferChunkFactory(),
        this.allocator,
        shared.getStorageProperties().getWriteBufferChunkValues(),
        shared.getStorageProperties().getWriteBufferSize(),
        shared.getStorageProperties().getWriteBufferTimeout().toMillis());
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<Field> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    Set<F> innerFields =
        fields.stream()
            .map(field -> (F) TagKVUtils.toFullName(ArrowFields.toColumnKey(field)))
            .collect(Collectors.toSet());
    AreaSet<K, F> areas = new AreaSet<>();
    areas.add(innerFields, ranges);

    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      Set<String> tables = tableIndex.find(areas);
      List<String> sortedTableNames = new ArrayList<>(tables);
      sortedTableNames.sort(Comparator.naturalOrder());

      for (String tableName : sortedTableNames) {
        try (Scanner<K, Scanner<F, V>> scanner =
            tableStorage.scan(tableName, innerFields, ranges)) {
          readBuffer.putRows(scanner);
        }
      }

      try (MemoryTable<K, F, T, V> activeTable =
          memtable.snapshot(new ArrayList<>(fields), (RangeSet<Long>) ranges, allocator)) {
        try (Scanner<K, Scanner<F, V>> scanner = activeTable.scan(innerFields, ranges, filter)) {
          readBuffer.putRows(scanner);
        }
      }
    } catch (IOException | StorageException e) {
      throw new RuntimeException(e);
    } finally {
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }

    return readBuffer.scanRows(innerFields, Range.all());
  }

  @Override
  public Set<Field> schema() throws StorageException {
    Map<String, DataType> types = (Map) tableIndex.getType();
    return ArrowFields.of(types);
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfRows(batch, schema, allocator), schema);
        }
      }
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfColumns(batch, schema, allocator), schema);
        }
      }
    }
  }

  private void putAll(@WillClose Iterable<UnorderedChunk.Snapshot> chunks, Map<F, T> schema)
      throws StorageException {
    try {
      checkBufferSize();
      commitLock.readLock().lock();
      deleteLock.readLock().lock();
      storageLock.readLock().lock();
      try {
        tableIndex.declareFields(schema);
        memtable.store(chunks);
      } finally {
        storageLock.readLock().unlock();
        deleteLock.readLock().unlock();
        commitLock.readLock().unlock();
      }
    } finally {
      chunks.forEach(UnorderedChunk.Snapshot::close);
    }
    if (timeout <= 0) {
      checkBufferTimeout();
    }
  }

  private void checkBufferSize() {
    checkLock.lock();
    try {
      if (!memtable.isOverloaded()) {
        return;
      }
      LOGGER.info("commit overloaded memtable");
      commitMemoryTable(true, false);
    } finally {
      checkLock.unlock();
    }
  }

  private void checkBufferTimeout() {
    CountDownLatch latch = new CountDownLatch(1);
    checkLock.lock();
    try {
      if (!memtable.isExpired()) {
        return;
      }

      LOGGER.info("commit expired memtable");
      commitMemoryTable(memtable.isOverloaded(), true);
    } finally {
      checkLock.unlock();
    }
  }

  private synchronized void commitMemoryTable(boolean switchTable, boolean sync) {
    commitLock.writeLock().lock();
    deleteLock.readLock().lock();
    try {

      MemoryTable snapshot =
          memtable.snapshot(
              new ArrayList<>(memtable.getFields()), ImmutableRangeSet.of(Range.all()), allocator);

      CountDownLatch latch = new CountDownLatch(sync ? 1 : 0);

      try (NoexceptAutoCloseableHolder<MemoryTable> holder = CloseableHolders.hold(snapshot)) {
        TableMeta<K, F, T, V> tableMeta = holder.peek().getMeta();

        String toDelete = previousTableName;
        String committedTableName =
            tableStorage.flush(
                holder.transfer(),
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
            "submit table {} to flush, with switch={}, latch={}",
            committedTableName,
            switchTable,
            latch);
        tableIndex.addTable(committedTableName, tableMeta);
        previousTableName = committedTableName;
      }

      memtable.setUntouched();

      if (switchTable) {
        LOGGER.info("reset write buffer");
        memtable.close();
        memtable = nextMemTable();
        previousTableName = null;
      }

      latch.await();
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.readLock().unlock();
      commitLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(AreaSet<K, Field> areas) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    storageLock.readLock().lock();
    try {
      LOGGER.info("start to delete {} in {}", areas, name);
      memtable.delete((AreaSet) areas);
      AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas((AreaSet) areas);
      Set<String> tables = tableIndex.find((AreaSet) innerAreas);
      tableStorage.delete(tables, (AreaSet) innerAreas);
      tableIndex.delete((AreaSet) innerAreas);
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
      tableStorage.clear();
      tableIndex.clear();
      memtable.close();
      memtable = nextMemTable();
      previousTableName = null;
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
      LOGGER.info("commit memtable before close");
      commitMemoryTable(false, true);
    }
    tableStorage.close();
    tableIndex.close();
    memtable.close();
    allocator.close();
  }
}
