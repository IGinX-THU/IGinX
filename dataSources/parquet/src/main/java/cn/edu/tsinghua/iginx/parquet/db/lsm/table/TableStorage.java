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

package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.SequenceGenerator;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: merge TableStorage, TableIndex and TombstoneStorage to control concurrent access to the
// storage
public class TableStorage implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final TableIndex tableIndex;
  private final ReadWriteLock storageLock = new ReentrantReadWriteLock(true);

  private final ExecutorService flusher;

  private final Map<String, MemoryTable> memTables = new HashMap<>();
  private final Map<String, AreaSet<Long, String>> memTombstones = new HashMap<>();
  private final Shared shared;
  private final ReadWriter readWriter;

  private final int localFlusherPermitsTotal;
  private final Semaphore localFlusherPermits;

  public TableStorage(Shared shared, ReadWriter readWriter) throws IOException {
    this.shared = shared;
    this.readWriter = readWriter;
    this.localFlusherPermitsTotal = shared.getFlusherPermits().availablePermits();
    this.localFlusherPermits = new Semaphore(localFlusherPermitsTotal, true);
    this.flusher =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("compact-" + readWriter.getName() + "-%d")
                .build());
    reload();
    this.tableIndex = new TableIndex(this);
  }

  private void reload() throws IOException {
    Iterable<String> tableNames = readWriter.tableNames();
    String last =
        StreamSupport.stream(tableNames.spliterator(), false)
            .max(Comparator.naturalOrder())
            .orElse("0");
    seqGen.reset(getSeq(last));
  }

  static long getSeq(String tableName) {
    return Long.parseLong(tableName, 10);
  }

  static String getTableName(long seq) {
    return String.format("%019d", seq);
  }

  public long nextTableId() {
    return seqGen.next();
  }

  public String flush(MemoryTable table, Runnable afterFlush) throws InterruptedException {
    shared.getFlusherPermits().acquire();
    localFlusherPermits.acquire();
    lock.writeLock().lock();
    try {
      String tableName = getTableName(seqGen.next());
      LOGGER.debug("waiting for flusher permit to flush {}", tableName);
      memTables.put(tableName, table);
      tableIndex.addTable(tableName, table.getMeta());
      flusher.submit(
          () -> {
            LOGGER.debug("task to flush {} started", tableName);
            try {
              TableMeta meta = table.getMeta();
              try (Scanner<Long, Scanner<String, Object>> scanner =
                  table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
                readWriter.flush(tableName, meta, scanner);
              }

              commitMemoryTable(tableName);

              LOGGER.debug("{} flushed", tableName);

              afterFlush.run();
            } catch (Throwable e) {
              LOGGER.error("failed to flush {}", tableName, e);
            } finally {
              localFlusherPermits.release();
              shared.getFlusherPermits().release();
              LOGGER.trace("unlock clean lock and released flusher permit");
            }
            LOGGER.debug("task to flush {} end", tableName);
          });
      return tableName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void commitMemoryTable(String name) throws IOException {
    lock.writeLock().lock();
    try {
      MemoryTable table = memTables.remove(name);
      if (table != null) {
        AreaSet<Long, String> tombstone = memTombstones.remove(name);
        if (tombstone != null) {
          readWriter.delete(name, tombstone);
        }
        table.close();
      } else {
        readWriter.delete(name);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void remove(String name) throws IOException {
    storageLock.writeLock().lock();
    try {
      tableIndex.removeTable(name);
      lock.writeLock().lock();
      try {
        MemoryTable table = memTables.remove(name);
        if (table != null) {
          memTombstones.remove(name);
          table.close();
        } else {
          readWriter.delete(name);
        }
      } finally {
        lock.writeLock().unlock();
      }
    } finally {
      storageLock.writeLock().unlock();
    }
  }

  public void clear() throws StorageException, InterruptedException {
    tableIndex.clear();
    localFlusherPermits.acquire(localFlusherPermitsTotal);
    lock.writeLock().lock();
    try {
      memTables.clear();
      memTombstones.clear();
      readWriter.clear();
      seqGen.reset();
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      localFlusherPermits.release(localFlusherPermitsTotal);
      lock.writeLock().unlock();
    }
  }

  private Scanner<Long, Scanner<String, Object>> scan(
      String tableName, Set<String> fields, RangeSet<Long> ranges) throws IOException {
    lock.readLock().lock();
    try {
      MemoryTable table = memTables.get(tableName);
      if (table != null) {
        AreaSet<Long, String> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          return table.scan(fields, ranges);
        }
        return new DeletedTable(table, tombstone).scan(fields, ranges);
      }
      return new FileTable(tableName, readWriter).scan(fields, ranges);
    } finally {
      lock.readLock().unlock();
    }
  }

  public TableMeta getMeta(String tableName) throws IOException {
    lock.readLock().lock();
    try {
      MemoryTable table = memTables.get(tableName);
      if (table != null) {
        TableMeta meta = table.getMeta();
        AreaSet<Long, String> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          return meta;
        }
        return new DeletedTableMeta(meta, tombstone);
      }
      return new FileTable(tableName, readWriter).getMeta();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(AreaSet<Long, String> areas) throws IOException {
    storageLock.readLock().lock();
    try {
      Set<String> tables = tableIndex.find(areas);
      tableIndex.delete(areas);
      lock.writeLock().lock();
      try {
        for (String tableName : tables) {
          if (memTables.containsKey(tableName)) {
            memTombstones.computeIfAbsent(tableName, k -> new AreaSet<>()).addAll(areas);
          } else {
            readWriter.delete(tableName, areas);
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
    } finally {
      storageLock.readLock().unlock();
    }
  }

  public Iterable<String> tableNames() throws IOException {
    return readWriter.tableNames();
  }

  @Override
  public void close() {
    flusher.shutdown();
  }

  public Map<String, DataType> schema() {
    return tableIndex.getType();
  }

  public Set<String> find(AreaSet<Long, String> areas) {
    return tableIndex.find(areas);
  }

  public void declareFields(Map<String, DataType> schema) throws TypeConflictedException {
    storageLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
    } finally {
      storageLock.readLock().unlock();
    }
  }

  public DataBuffer<Long, String, Object> query(
      Set<String> fields, RangeSet<Long> ranges, Filter filter)
      throws StorageException, IOException {

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();

    storageLock.readLock().lock();
    try {
      Set<String> tables = tableIndex.find(areas);
      List<String> sortedTableNames = new ArrayList<>(tables);
      sortedTableNames.sort(Comparator.naturalOrder());

      for (String tableName : sortedTableNames) {
        try (Scanner<Long, Scanner<String, Object>> scanner = scan(tableName, fields, ranges)) {
          buffer.putRows(scanner);
        }
      }

      return buffer;
    } finally {
      storageLock.readLock().unlock();
    }
  }
}
