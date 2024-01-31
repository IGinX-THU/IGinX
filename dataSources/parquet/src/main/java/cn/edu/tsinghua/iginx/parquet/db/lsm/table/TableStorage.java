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
import cn.edu.tsinghua.iginx.parquet.utils.StorageProperties;
import cn.edu.tsinghua.iginx.parquet.utils.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.utils.exception.StorageException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStorage<K extends Comparable<K>, F, T, V> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class.getName());

  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock cleanLock = new ReentrantReadWriteLock(true);

  private final Map<String, TableEntity<K, F, T, V>> tables = new HashMap<>();

  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final ExecutorService flusher = Executors.newCachedThreadPool();

  private final StorageProperties props;

  private final ReadWriter<K, F, T, V> readWriter;

  public TableStorage(StorageProperties props, ReadWriter<K, F, T, V> readWriter)
      throws IOException {
    this.props = props;
    this.readWriter = readWriter;
    reload();
  }

  private void reload() throws IOException {
    for (String tableName : readWriter.tableNames()) {
      TableEntity<K, F, T, V> entity = new TableEntity<>(tableName, readWriter);
      tables.put(tableName, entity);
    }
  }

  public String flush(Table<K, F, T, V> table) {
    lock.writeLock().lock();
    try {
      props.getFlusherPermits().acquireUninterruptibly();
      String tableName = String.valueOf(seqGen.next());
      TableEntity<K, F, T, V> entity = new TableEntity<>(tableName, table, readWriter);
      flusher.submit(
          () -> {
            cleanLock.readLock().lock();
            try {
              LOGGER.trace("start to flush");
              entity.flush();
            } catch (Throwable e) {
              LOGGER.error("failed to flush {}", tableName, e);
            } finally {
              cleanLock.readLock().unlock();
              props.getFlusherPermits().release();
              LOGGER.trace("unlock clean lock and released flusher permit");
            }
          });
      tables.put(tableName, entity);
      return tableName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() throws StorageException {
    cleanLock.writeLock().lock();
    try {
      tables.clear();
      seqGen.reset();
      readWriter.clear();
    } catch (IOException e) {
      throw new StorageException(e);
    } finally {
      cleanLock.writeLock().unlock();
    }
  }

  public Table<K, F, T, V> get(String tableName) {
    lock.readLock().lock();
    try {
      TableEntity<K, F, T, V> entity = tables.get(tableName);
      if (entity == null) {
        throw new NotIntegrityException("table " + tableName + " not exists");
      }
      return entity.data();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Iterable<String> tableNames() throws IOException {
    return readWriter.tableNames();
  }

  private static class TableEntity<K extends Comparable<K>, F, T, V> {
    private final String tableName;

    private final ReadWriter<K, F, T, V> readWriter;

    private final AtomicReference<Table<K, F, T, V>> toWrite;

    public TableEntity(String tableName, ReadWriter<K, F, T, V> readWriter) {
      this(tableName, new FileTable<>(tableName, readWriter), readWriter);
    }

    public TableEntity(
        String tableName, Table<K, F, T, V> toWrite, ReadWriter<K, F, T, V> readWriter) {
      this.tableName = tableName;
      this.readWriter = readWriter;
      this.toWrite = new AtomicReference<>(toWrite);
    }

    public void flush() throws IOException, StorageException {
      Table<K, F, T, V> table = toWrite.get();
      TableMeta<K, F, T, V> meta = table.getMeta();
      try (Scanner<K, Scanner<F, V>> scanner =
          table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
        readWriter.flush(tableName, scanner, meta.getSchema());
      }
      toWrite.set(new FileTable<>(tableName, readWriter));
    }

    public Table<K, F, T, V> data() {
      return toWrite.get();
    }
  }

  @Override
  public void close() {
    flusher.shutdown();
  }
}
