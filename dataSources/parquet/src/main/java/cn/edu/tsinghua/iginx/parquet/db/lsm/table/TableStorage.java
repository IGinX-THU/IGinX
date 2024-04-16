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
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final ExecutorService flusher = Executors.newCachedThreadPool();

  private final Map<String, MemoryTable<K, F, T, V>> memTables = new HashMap<>();
  private final Map<String, AreaSet<K, F>> memTombstones = new HashMap<>();
  private final Shared shared;
  private final ReadWriter<K, F, T, V> readWriter;

  private final int localFlusherPermitsTotal;
  private final Semaphore localFlusherPermits;

  public TableStorage(Shared shared, ReadWriter<K, F, T, V> readWriter) throws IOException {
    this.shared = shared;
    this.readWriter = readWriter;
    this.localFlusherPermitsTotal = shared.getFlusherPermits().availablePermits();
    this.localFlusherPermits = new Semaphore(localFlusherPermitsTotal, true);
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

  public long nextTableId() {
    return seqGen.next();
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
            LOGGER.debug("task to flush {} started", tableName);
            try {
              TableMeta<K, F, T, V> meta = table.getMeta();
              try (Scanner<K, Scanner<F, V>> scanner =
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
      MemoryTable<K, F, T, V> table = memTables.remove(name);
      if (table != null) {
        AreaSet<K, F> tombstone = memTombstones.remove(name);
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
    lock.writeLock().lock();
    try {
      MemoryTable<K, F, T, V> table = memTables.remove(name);
      if (table != null) {
        memTombstones.remove(name);
        table.close();
      } else {
        readWriter.delete(name);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() throws StorageException, InterruptedException {
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

  public Scanner<K, Scanner<F, V>> scan(String tableName, Set<F> fields, RangeSet<K> ranges)
      throws IOException {
    lock.readLock().lock();
    try {
      MemoryTable<K, F, T, V> table = memTables.get(tableName);
      if (table != null) {
        AreaSet<K, F> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          return table.scan(fields, ranges);
        }
        return new DeletedTable<>(table, tombstone).scan(fields, ranges);
      }
      return new FileTable<>(tableName, readWriter).scan(fields, ranges);
    } finally {
      lock.readLock().unlock();
    }
  }

  public TableMeta<K, F, T, V> getMeta(String tableName) throws IOException {
    lock.readLock().lock();
    try {
      MemoryTable<K, F, T, V> table = memTables.get(tableName);
      if (table != null) {
        TableMeta<K, F, T, V> meta = table.getMeta();
        AreaSet<K, F> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          return meta;
        }
        return new DeletedTableMeta<>(meta, tombstone);
      }
      return new FileTable<>(tableName, readWriter).getMeta();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(Set<String> tables, AreaSet<K, F> areas) throws IOException {
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
  }

  public Iterable<String> tableNames() throws IOException {
    return readWriter.tableNames();
  }

  @Override
  public void close() {
    flusher.shutdown();
  }
}
