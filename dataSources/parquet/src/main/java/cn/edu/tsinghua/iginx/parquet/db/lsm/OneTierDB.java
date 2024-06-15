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
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.compact.Flusher;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.WriteBatches;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseables;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.WillClose;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class OneTierDB implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final String name;
  private final Shared shared;
  private final BufferAllocator allocator;
  private final TableStorage tableStorage;
  private final MemTableQueue memTableQueue;
  private final Flusher flusher;

  public OneTierDB(String name, Shared shared, ReadWriter readerWriter) throws IOException {
    this.name = name;
    this.shared = shared;
    this.allocator = shared.getAllocator().newChildAllocator(name, 0, Long.MAX_VALUE);
    this.tableStorage = new TableStorage(shared, readerWriter);
    this.memTableQueue = new MemTableQueue(shared, allocator);
    this.flusher = new Flusher(name, shared, allocator, memTableQueue, tableStorage);
  }

  @Override
  public Scanner<Long, Scanner<String, Object>> query(
      Set<Field> fields, RangeSet<Long> ranges, Filter filter) throws IOException, StorageException {
    Set<String> innerFields =
        fields.stream()
            .map(field -> TagKVUtils.toFullName(ArrowFields.toColumnKey(field)))
            .collect(Collectors.toSet());

    lock.readLock().lock();
    try {
      List<Scanner<Long, Scanner<String, Object>>> inMemories =
          memTableQueue.scan(new ArrayList<>(fields), ranges, allocator);
      try (AutoCloseable c = AutoCloseables.all(inMemories)) {
        DataBuffer<Long, String, Object> readBuffer = tableStorage.query(innerFields, ranges, filter);
        for (Scanner<Long, Scanner<String, Object>> scanner : inMemories) {
          readBuffer.putRows(scanner);
        }
        return readBuffer.scanRows(innerFields, Range.all());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Long> count(Set<Field> fields) throws InterruptedException, IOException, StorageException {
    lock.readLock().lock();
    try {
      // to simplify the implementation, we flush the memTableQueue before counting
      memTableQueue.compact();
      memTableQueue.flush();
      Set<String> innerFields =
          fields.stream()
              .map(field -> TagKVUtils.toFullName(ArrowFields.toColumnKey(field)))
              .collect(Collectors.toSet());
      return tableStorage.count(innerFields);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<Field> schema() throws StorageException {
    lock.readLock().lock();
    try {
      Map<String, DataType> types = tableStorage.schema();
      return ArrowFields.of(types);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void upsertRows(
      Scanner<Long, Scanner<String, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException {
    try (Scanner<Long, Scanner<Long, Scanner<String, Object>>> batchScanner =
             new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<Long, Scanner<String, Object>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfRows(batch, schema, allocator), schema);
        }
      }
    }
  }

  @Override
  public void upsertColumns(
      Scanner<String, Scanner<Long, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException {
    try (Scanner<Long, Scanner<String, Scanner<Long, Object>>> batchScanner =
             new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<String, Scanner<Long, Object>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfColumns(batch, schema, allocator), schema);
        }
      }
    }
  }

  private void putAll(@WillClose Iterable<Chunk.Snapshot> chunks, Map<String, DataType> schema)
      throws TypeConflictedException, InterruptedException {
    lock.readLock().lock();
    try (NoexceptAutoCloseable guarder = NoexceptAutoCloseables.all(chunks)) {
      tableStorage.declareFields(schema);
      memTableQueue.store(chunks);
      if (shared.getStorageProperties().getWriteBufferTimeout().toMillis() <= 0) {
        memTableQueue.flush();
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void delete(AreaSet<Long, Field> range) throws StorageException {
    AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas(range);
    lock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} in {}", range, name);
      memTableQueue.delete(range);
      tableStorage.delete(innerAreas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void clear() throws StorageException {
    lock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", name);
      flusher.stop();
      memTableQueue.clear();
      tableStorage.clear();
      if (allocator.getAllocatedMemory() > 0) {
        throw new IllegalStateException("allocator is not empty: " + allocator.toVerboseString());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("cleared {}, allocator: {}", name, allocator);
      }
      flusher.start();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    lock.writeLock().lock();
    try {
      if (shared.getStorageProperties().toFlushOnClose()) {
        memTableQueue.flush();
      }
      flusher.close();
      memTableQueue.close();
      tableStorage.close();
      allocator.close();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
