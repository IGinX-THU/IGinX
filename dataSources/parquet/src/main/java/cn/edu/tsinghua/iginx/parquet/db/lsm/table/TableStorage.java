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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.shared.Shared;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStorage<K extends Comparable<K>, F, T, V> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class.getName());
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock cleanLock = new ReentrantReadWriteLock(true);

  private final ConcurrentMap<String, Table<K, F, T, V>> memTables = new ConcurrentHashMap<>();

  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final ExecutorService flusher = Executors.newCachedThreadPool();

  private final Shared shared;

  private final ReadWriter<K, F, T, V> readWriter;

  public TableStorage(Shared shared, ReadWriter<K, F, T, V> readWriter) throws IOException {
    this.shared = shared;
    this.readWriter = readWriter;
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

  public String flush(Table<K, F, T, V> table) {
    lock.writeLock().lock();
    try {
      String tableName = getTableName(seqGen.next());
      LOGGER.debug("waiting for flusher permit to flush {}", tableName);
      shared.getFlusherPermits().acquireUninterruptibly();
      memTables.put(tableName, table);
      flusher.submit(
          () -> {
            cleanLock.readLock().lock();
            try {
              LOGGER.trace("start to flush");
              TableMeta<K, F, T, V> meta = table.getMeta();
              try (Scanner<K, Scanner<F, V>> scanner =
                  table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
                readWriter.flush(tableName, meta, scanner);
              }
              memTables.remove(tableName);
            } catch (Throwable e) {
              LOGGER.error("failed to flush {}", tableName, e);
            } finally {
              cleanLock.readLock().unlock();
              shared.getFlusherPermits().release();
              LOGGER.trace("unlock clean lock and released flusher permit");
            }
          });
      return tableName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() throws StorageException {
    cleanLock.writeLock().lock();
    try {
      memTables.clear();
      readWriter.clear();
      seqGen.reset();
    } catch (IOException e) {
      throw new StorageException(e);
    } finally {
      cleanLock.writeLock().unlock();
    }
  }

  public Table<K, F, T, V> get(String tableName) {
    lock.readLock().lock();
    try {
      Table<K, F, T, V> table = memTables.get(tableName);
      if (table != null) {
        return table;
      }
      return new FileTable<>(tableName, readWriter);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Iterable<String> tableNames() throws IOException {
    return readWriter.tableNames();
  }

  @Override
  public void close() {
    flusher.shutdown();
  }

  private static class SequenceGenerator {

    private final AtomicLong current = new AtomicLong();

    public long next() {
      return current.incrementAndGet();
    }

    public void reset() {
      reset(new AtomicLong().get());
    }

    public void reset(long last) {
      current.set(last);
    }
  }
}
