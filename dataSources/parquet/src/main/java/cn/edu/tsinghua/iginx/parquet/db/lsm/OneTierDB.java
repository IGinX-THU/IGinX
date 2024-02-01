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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.index.TableIndex;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.Tombstone;
import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.TombstoneStorage;
import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import cn.edu.tsinghua.iginx.parquet.shared.Shared;
import cn.edu.tsinghua.iginx.parquet.shared.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
  private final Shared shared;
  private final TableStorage<K, F, T, V> tableStorage;
  private final TombstoneStorage<K, F> tombstoneStorage;
  private final TableIndex<K, F, T, V> tableIndex;

  private DataBuffer<K, F, V> writeBuffer = new DataBuffer<>();
  private final LongAdder inserted = new LongAdder();
  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock commitLock = new ReentrantReadWriteLock(true);

  public OneTierDB(Shared shared, Path dir, ReadWriter<K, F, T, V> readerWriter)
      throws IOException {
    this.shared = shared;
    this.tableStorage = new TableStorage<>(shared, readerWriter);
    this.tombstoneStorage =
        new TombstoneStorage<>(
            shared,
            dir.resolve(Constants.DIR_NAME_TOMBSTONE),
            readerWriter.getKeyFormat(),
            readerWriter.getFieldFormat());
    this.tableIndex = new TableIndex<>(tableStorage, tombstoneStorage);
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    try {
      Set<String> tables = tableIndex.find(fields, ranges);
      List<String> sortedTableNames = new ArrayList<>(tables);
      sortedTableNames.sort(Comparator.naturalOrder());
      for (String tableName : sortedTableNames) {
        Table<K, F, T, V> table = tableStorage.get(tableName);
        Tombstone<K, F> tombstone = tombstoneStorage.get(tableName);
        try (Scanner<K, Scanner<F, V>> scanner = table.scan(fields, ranges)) {
          readBuffer.putRows(scanner);
        }
        Tombstone.playback(tombstone, readBuffer);
      }

      for (Range<K> range : ranges.asRanges()) {
        readBuffer.putRows(writeBuffer.scanRows(fields, range));
      }
    } catch (IOException | StorageException e) {
      throw new RuntimeException(e);
    } finally {
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }

    return readBuffer.scanRows(fields, Range.all());
  }

  @Override
  public Optional<Range<K>> range() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
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
    checkBufferSize();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
            writeBuffer.putRows(batch);
            inserted.add(batchScanner.key());
          }
        }
      }
    } finally {
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    checkBufferSize();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
            writeBuffer.putColumns(batch);
            inserted.add(batchScanner.key());
          }
        }
      }
    } finally {
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  private void checkBufferSize() throws StorageException {
    if (inserted.sum() < shared.getStorageProperties().getWriteBufferSize()) {
      return;
    }
    synchronized (inserted) {
      if (inserted.sum() < shared.getStorageProperties().getWriteBufferSize()) {
        return;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "flushing is triggered when write buffer {} reaching {}",
            inserted.sum(),
            shared.getStorageProperties().getWriteBufferSize());
      }
      commitMemoryTable();
      inserted.reset();
    }
  }

  private void commitMemoryTable() throws StorageException {
    commitLock.writeLock().lock();
    deleteLock.writeLock().lock();
    try {
      Map<F, T> types =
          tableIndex.getType(
              writeBuffer.fields(),
              f -> {
                throw new NotIntegrityException("field " + f + " is not found in schema");
              });
      MemoryTable<K, F, T, V> table = new MemoryTable<>(writeBuffer, types);
      String name = tableStorage.flush(table);
      tableIndex.addTable(name, table.getMeta());
      this.writeBuffer = new DataBuffer<>();
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(Set<F> fields, RangeSet<K> ranges) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      writeBuffer.remove(fields, ranges);
      Set<String> tables = tableIndex.find(fields, ranges);
      tombstoneStorage.delete(
          tables,
          tombstone -> {
            tombstone.delete(fields, ranges);
          });
      tableIndex.delete(fields, ranges);
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void delete(Set<F> fields) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      writeBuffer.remove(fields);
      Set<String> tables = tableIndex.find(fields);
      tombstoneStorage.delete(
          tables,
          tombstone -> {
            tombstone.delete(fields);
          });
      tableIndex.delete(fields);
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void delete(RangeSet<K> ranges) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      writeBuffer.remove(ranges);
      Set<String> tables = tableIndex.find(ranges);
      tombstoneStorage.delete(
          tables,
          tombstone -> {
            tombstone.delete(ranges);
          });
      tableIndex.delete(ranges);
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void clear() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      tableStorage.clear();
      tombstoneStorage.clear();
      tableIndex.clear();
      writeBuffer.clear();
      inserted.reset();
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("flushing is triggered when closing");
    commitMemoryTable();
    tableStorage.close();
    tombstoneStorage.close();
    tableIndex.close();
  }
}
