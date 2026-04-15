/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.InMemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.WriteBatch;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.Catalog;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseables;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.Shared;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);

  private final Path path;
  private final Shared shared;

  private final Catalog catalog;
  private final TableStorage tableStorage;
  private final MemTableQueue memTableQueue;
  private final Compactor flusher;
  private final Indexer indexer;
  private final ScanExecutor scanExecutor;

  public OneTierDB(Shared shared, Path path) {
    this.path = path;
    this.shared = shared;
    this.catalog = new Catalog(shared.getConfig().getCatalog());
    this.indexer = new Indexer(path.toString(), shared.getFlusherPermits());
    this.tableStorage =
        new TableStorage(
            path, shared.getConfig().getStorage(), catalog, shared.getCachePool(), indexer);
    this.memTableQueue =
        new MemTableQueue(
            path.toString(),
            shared.getConfig().getMemtable(),
            shared.getMemTablePermits(),
            shared.getAllocator());
    this.flusher =
        new Compactor(
            path.toString(),
            shared.getFlusherPermits(),
            shared.getConfig().getMemtable().getTimeout(),
            memTableQueue,
            tableStorage);
    this.scanExecutor = new ScanExecutor(path.toString(), shared.getScannerPermits());
    indexer.start();
    flusher.start();
    if (shared.getConfig().isFlushOnClose()) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      close();
                    } catch (Throwable e) {
                      LOGGER.error("failed to closing db {} in shutdown hook", path, e);
                    }
                  },
                  OneTierDB.class.getSimpleName() + "(" + path + ")-ShutdownHook"));
    }
  }

  public RowStream scan(List<String> patterns, @Nullable TagFilter tagFilter, Filter filter)
      throws StorageException {
    deleteLock.readLock().lock();
    try {
      List<Field> fields = ImmutableList.copyOf(catalog.findFields(patterns, tagFilter));
      RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
      List<InMemoryTable> inMemoryTables = memTableQueue.snapshot(fields);
      try (NoexceptAutoCloseable ignored = NoexceptAutoCloseables.all(inMemoryTables)) {
        List<Table> fileTables = tableStorage.load(fields, rangeSet);
        List<Table> allHitTables =
            Streams.concat(fileTables.stream(), inMemoryTables.stream())
                .collect(ImmutableList.toImmutableList());
        return scanExecutor.scan(allHitTables, fields, filter);
      }
    } catch (IOException | PhysicalException e) {
      LOGGER.debug(
          "Query{patterns: {}, tagFilter: {}, filter: {}} failed", patterns, tagFilter, filter, e);
      throw new StorageException(e);
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  public Set<Field> schema(List<String> patterns, @Nullable TagFilter tagFilter)
      throws StorageException {
    deleteLock.readLock().lock();
    try {
      return catalog.findFields(patterns, tagFilter);
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  public void insert(DataView data) throws StorageException, InterruptedException {
    try (WriteBatch batch = memTableQueue.prepare(data)) {
      deleteLock.readLock().lock();
      try {
        Set<Field> fields = batch.getFields();
        catalog.verifyAndInsertFields(fields);
        memTableQueue.store(batch);
        if (shared.getConfig().getMemtable().getTimeout().toMillis() <= 0) {
          memTableQueue.flushAll(false);
        }
      } finally {
        deleteLock.readLock().unlock();
      }
    }
  }

  public void delete(List<String> patterns, @Nullable TagFilter tagFilter, RangeSet<Long> ranges)
      throws StorageException, InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug(
          "request to delete from {} where {} with {} in {}", patterns, ranges, tagFilter, path);
      Set<Field> fields = catalog.findFields(patterns, tagFilter);
      if (ranges.encloses(Range.all())) {
        if (Patterns.isAll(patterns) && tagFilter == null) {
          clear();
        } else {
          deleteFields(fields);
        }
      } else {
        deleteRanges(fields, ranges);
      }
    } catch (IOException e) {
      throw new StorageException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteRanges(Set<Field> fields, RangeSet<Long> keyRangeSet)
      throws InterruptedException, IOException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} where {} in {}", fields, keyRangeSet, path);
      memTableQueue.flushAll(true);
      tableStorage.delete(fields, keyRangeSet);
      catalog.delete(fields, keyRangeSet);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteFields(Set<Field> fields)
      throws StorageException, IOException, InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} in {}", fields, path);
      memTableQueue.flushAll(true);
      tableStorage.delete(fields);
      catalog.delete(fields);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void clear() throws InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", path);
      flusher.stop();
      indexer.stop();
      catalog.clear();
      memTableQueue.clear();
      tableStorage.clear();
      indexer.start();
      flusher.start();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.info("flushing {}", path);
      memTableQueue.flushAll(true);
      flusher.stop();
      indexer.stop();
      memTableQueue.close();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }
}
