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

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.SequenceGenerator;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.StorageShared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
public class TableStorage<K extends Comparable<K>, F, T, V> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class.getName());
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final ExecutorService flusher;

  private final StorageShared shared;

  private final Map<String, MemoryTable<K, F, T, V>> memTables = new HashMap<>();
  private final Map<String, AreaSet<K, F>> memTombstones = new HashMap<>();

  private final ReadWriter<K, F, T, V> readWriter;

  private final int localFlusherPermitsTotal;
  private final Semaphore localFlusherPermits;
  private final String name;

  public TableStorage(String name, StorageShared shared, ReadWriter<K, F, T, V> readWriter)
      throws IOException {
    this.name = name;
    this.shared = shared;
    this.readWriter = readWriter;
    this.localFlusherPermitsTotal = shared.getFlusherPermits().availablePermits();
    this.localFlusherPermits = new Semaphore(localFlusherPermitsTotal, true);
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("ParquetFlusher(" + name + ")-%d");
    this.flusher = Executors.newSingleThreadExecutor(builder.build());
    reload();
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

  public String flush(MemoryTable<K, F, T, V> table, Runnable afterFlush)
      throws InterruptedException {
    shared.getFlusherPermits().acquire();
    localFlusherPermits.acquire();
    lock.writeLock().lock();
    try {
      String tableName = getTableName(seqGen.next());
      LOGGER.debug("waiting for flusher permit to flush {}", tableName);
      memTables.put(tableName, table);
      flusher.submit(
          () -> {
            try {
              LOGGER.debug("start to flush {} of {} started", tableName, name);
              TableMeta<K, F, T, V> meta = table.getMeta();
              try (Scanner<K, Scanner<F, V>> scanner =
                  table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
                readWriter.flush(tableName, meta, scanner);
              }

              commitMemoryTable(tableName);

              LOGGER.trace("run afterFlush Hook");

              afterFlush.run();
            } catch (Throwable e) {
              LOGGER.error("failed to flush {}", tableName, e);
            } finally {
              localFlusherPermits.release();
              shared.getFlusherPermits().release();
            }
            LOGGER.debug("flush success {}", tableName);
          });
      return tableName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void commitMemoryTable(String tableName) throws IOException {
    lock.writeLock().lock();
    try {
      LOGGER.debug("commit table {} of {}", tableName, name);
      if (memTables.remove(tableName) != null) {
        LOGGER.trace("table is in memory, commit it to file");
        AreaSet<K, F> tombstone = memTombstones.remove(tableName);
        if (tombstone != null) {
          LOGGER.trace("table has tombstone in memory, commit it to file");
          readWriter.delete(tableName, tombstone);
        }
      } else {
        LOGGER.trace("table is not in memory, delete it from file");
        readWriter.delete(tableName);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void remove(String tableName) {
    lock.writeLock().lock();
    try {
      LOGGER.debug("remove table {} of {}", tableName, name);
      if (memTables.remove(tableName) != null) {
        LOGGER.trace("table is in memory, remove it from memory");
        memTombstones.remove(tableName);
      } else {
        LOGGER.trace("table is not in memory, delete it from file");
        readWriter.delete(tableName);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() throws StorageException, InterruptedException {
    localFlusherPermits.acquire(localFlusherPermitsTotal);
    lock.writeLock().lock();
    try {
      LOGGER.trace("clear table storage of {}", name);
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

  public Table<K, F, T, V> get(String tableName) throws IOException {
    lock.readLock().lock();
    try {
      LOGGER.debug("get table {} of {}", tableName, name);
      MemoryTable<K, F, T, V> table = memTables.get(tableName);
      if (table != null) {
        LOGGER.trace("table is in memory");
        AreaSet<K, F> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          LOGGER.trace("table has no tombstone in memory");
          return table;
        }
        LOGGER.trace("table has tombstone in memory");
        return new DeletedTable<>(table, tombstone);
      }
      return new FileTable<>(tableName, readWriter);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(Set<String> tables, AreaSet<K, F> areas) throws IOException {
    lock.writeLock().lock();
    try {
      LOGGER.debug("delete areas {} from tables {} of {}", areas, tables, name);
      for (String tableName : tables) {
        MemoryTable<K, F, T, V> table = memTables.get(tableName);
        if (table != null) {
          LOGGER.trace("table is in memory, delete areas from it");
          memTombstones.computeIfAbsent(tableName, k -> new AreaSet<>()).addAll(areas);
        } else {
          LOGGER.trace("table is not in memory, delete areas from file");
          readWriter.delete(tableName, areas);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Iterable<String> tableNames() throws IOException {
    LOGGER.trace("get table names of {}", name);
    return readWriter.tableNames();
  }

  @Override
  public void close() {
    LOGGER.trace("close table storage of {}", name);
    flusher.shutdown();
  }
}
