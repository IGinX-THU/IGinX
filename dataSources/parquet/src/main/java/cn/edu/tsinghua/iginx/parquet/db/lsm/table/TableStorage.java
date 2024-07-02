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
      if (memTables.remove(name) != null) {
        AreaSet<K, F> tombstone = memTombstones.remove(name);
        if (tombstone != null) {
          readWriter.delete(name, tombstone);
        }
      } else {
        readWriter.delete(name);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void remove(String name) {
    lock.writeLock().lock();
    try {
      if (memTables.remove(name) != null) {
        memTombstones.remove(name);
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

  public Table<K, F, T, V> get(String tableName) throws IOException {
    lock.readLock().lock();
    try {
      MemoryTable<K, F, T, V> table = memTables.get(tableName);
      if (table != null) {
        AreaSet<K, F> tombstone = memTombstones.get(tableName);
        if (tombstone == null) {
          return table;
        }
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
      for (String tableName : tables) {
        MemoryTable<K, F, T, V> table = memTables.get(tableName);
        if (table != null) {
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
