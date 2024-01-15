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
import cn.edu.tsinghua.iginx.parquet.common.Config;
import cn.edu.tsinghua.iginx.parquet.common.Constants;
import cn.edu.tsinghua.iginx.parquet.common.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.common.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.scanner.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.scanner.ConcatScanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.utils.SerializeUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
  private DataBuffer<K, F, V> writtenBuffer = new DataBuffer<>();
  private RangeTombstone<K, F> writtenDeleted = new RangeTombstone<>();
  private final RangeSet<K> ranges = TreeRangeSet.create();
  private final ConcurrentHashMap<F, T> schema = new ConcurrentHashMap<>();
  private final ReadWriteLock deletedLock = new ReentrantReadWriteLock(true);
  private final LongAdder inserted = new LongAdder();
  private final AppendQueue<K, F, V, T> appendQueue;
  private final ReadWriter<K, F, V, T> readerWriter;

  private final Config config;

  public OneTierDB(Config config, Path dir, ReadWriter<K, F, V, T> readerWriter)
      throws IOException {
    this.config = config;
    this.readerWriter = readerWriter;
    this.appendQueue = new AppendQueue<>(dir, readerWriter, 1);
    reload();
  }

  private void reload() throws IOException {
    Iterator<Table<K, F, V, T>> iterator = appendQueue.iterator();
    if (!iterator.hasNext()) {
      return;
    }

    Table<K, F, V, T> lastTable = Iterators.getLast(iterator);
    TableMeta<F, T> meta = lastTable.getMeta();
    schema.putAll(meta.getSchema());
    String keyRangeStr = meta.getExtra().get(Constants.KEY_RANGE_NAME);
    if (keyRangeStr == null) {
      throw new RuntimeException("key range is not found in file of " + lastTable);
    }
    RangeSet<K> ranges =
        SerializeUtils.deserializeRangeSet(keyRangeStr, readerWriter.getKeyFormat());
    this.ranges.addAll(ranges);
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    try {
      for (Table<K, F, V, T> table : appendQueue) {
        TableMeta<F, T> meta = table.getMeta();
        Map<String, String> extra = meta.getExtra();
        String rangeTombstoneStr = extra.get(Constants.TOMBSTONE_NAME);
        if (rangeTombstoneStr == null) {
          throw new RuntimeException("tombstone is not found in file of " + table);
        }
        RangeTombstone<K, F> rangeTombstone =
            SerializeUtils.deserializeRangeTombstone(
                rangeTombstoneStr, readerWriter.getKeyFormat(), readerWriter.getFieldFormat());
        RangeTombstone.playback(rangeTombstone, readBuffer);
        for (Range<K> range : ranges.asRanges()) {
          try (Scanner<K, Scanner<F, V>> scanner = table.scan(fields, range)) {
            readBuffer.putRows(scanner);
          }
        }
      }
    } catch (IOException | StorageException e) {
      throw new RuntimeException(e);
    }

    deletedLock.readLock().lock();
    try {
      RangeTombstone.playback(writtenDeleted, readBuffer);
      for (Range<K> range : ranges.asRanges()) {
        readBuffer.putRows(writtenBuffer.scanRows(fields, range));
      }
    } finally {
      deletedLock.readLock().unlock();
    }

    List<Scanner<K, Scanner<F, V>>> scanners = new ArrayList<>();
    for (Range<K> range : ranges.asRanges()) {
      scanners.add(readBuffer.scanRows(fields, range));
    }
    return new ConcatScanner<>(scanners.iterator());
  }

  @Override
  public Optional<Range<K>> range() throws StorageException {
    deletedLock.readLock().lock();
    try {
      RangeSet<K> rangeSet = TreeRangeSet.create(ranges.asRanges());
      RangeTombstone.playback(writtenDeleted, rangeSet);
      writtenBuffer.range().ifPresent(rangeSet::add);

      if (rangeSet.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(rangeSet.span());
      }
    } finally {
      deletedLock.readLock().unlock();
    }
  }

  @Override
  public Map<F, T> schema() throws StorageException {
    return Collections.unmodifiableMap(schema);
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, config.getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        checkBufferSize();
        deletedLock.readLock().lock();
        try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
          checkSchema(schema);
          writtenBuffer.putRows(batch);
          inserted.add(batchScanner.key());
        } finally {
          deletedLock.readLock().unlock();
        }
      }
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, config.getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        checkBufferSize();
        deletedLock.readLock().lock();
        try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
          checkSchema(schema);
          writtenBuffer.putColumns(batch);
          inserted.add(batchScanner.key());
        } finally {
          deletedLock.readLock().unlock();
        }
      }
    }
  }

  private void checkSchema(Map<F, T> schema) throws StorageException {
    for (Map.Entry<F, T> entry : schema.entrySet()) {
      T old = this.schema.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        throw new TypeConflictedException(
            entry.getKey().toString(), entry.getValue().toString(), old.toString());
      }
    }
  }

  private void checkBufferSize() {
    if (inserted.sum() < config.getWriteBufferSize()) {
      return;
    }

    synchronized (inserted) {
      if (inserted.sum() < config.getWriteBufferSize()) {
        return;
      }
      LOGGER.info(
          "flushing is triggered when write buffer reaching {}", config.getWriteBufferSize());
      commitMemoryTable();
    }
  }

  private void commitMemoryTable() {
    deletedLock.writeLock().lock();
    try {
      Map<String, String> extra = getExtra(writtenDeleted, ranges);
      MemoryTable<K, F, V, T> flushedTable = new MemoryTable<>(writtenBuffer, schema, extra);
      appendQueue.offer(flushedTable);

      RangeTombstone.playback(writtenDeleted, ranges);
      this.writtenBuffer.range().ifPresent(ranges::add);
      if (!ranges.isEmpty()) {
        ranges.add(ranges.span());
      }
      this.writtenBuffer = new DataBuffer<>();
      this.writtenDeleted = new RangeTombstone<>();
      this.inserted.reset();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      deletedLock.writeLock().unlock();
    }
  }

  @Nonnull
  private Map<String, String> getExtra(
      RangeTombstone<K, F> flushedDeleted, RangeSet<K> flushedRangeSet) {
    Map<String, String> extra = new HashMap<>();
    extra.put(
        Constants.TOMBSTONE_NAME,
        SerializeUtils.serialize(
            flushedDeleted, readerWriter.getKeyFormat(), readerWriter.getFieldFormat()));
    extra.put(
        Constants.KEY_RANGE_NAME,
        SerializeUtils.serialize(flushedRangeSet, readerWriter.getKeyFormat()));
    return extra;
  }

  @Override
  public void delete(Set<F> fields, RangeSet<K> ranges) throws StorageException {
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove(fields, ranges);
      writtenDeleted.delete(fields, ranges);
    } finally {
      deletedLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(Set<F> fields) throws StorageException {
    deletedLock.writeLock().lock();
    try {
      schema.keySet().removeAll(fields);
      writtenBuffer.remove(fields);
      writtenDeleted.delete(fields);
    } finally {
      deletedLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(RangeSet<K> ranges) throws StorageException {
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove(ranges);
      writtenDeleted.delete(ranges);
    } finally {
      deletedLock.writeLock().unlock();
    }
  }

  @Override
  public void delete() throws StorageException {
    deletedLock.writeLock().lock();
    try {
      appendQueue.clear();
      writtenBuffer.remove();
      writtenDeleted.reset();
      schema.clear();
      ranges.clear();
    } finally {
      deletedLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("flushing is triggered when closing");
    deletedLock.writeLock().lock();
    try {
      commitMemoryTable();
      appendQueue.close();
    } finally {
      deletedLock.writeLock().unlock();
    }
  }
}
