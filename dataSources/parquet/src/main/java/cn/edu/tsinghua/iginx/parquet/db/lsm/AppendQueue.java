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

import cn.edu.tsinghua.iginx.parquet.common.Constants;
import cn.edu.tsinghua.iginx.parquet.common.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.FileTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableMeta;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AppendQueue<K extends Comparable<K>, F, V, T>
    implements Iterable<Table<K, F, V, T>>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AppendQueue.class.getName());

  private final ConcurrentLinkedQueue<TableEntity<K, F, V, T>> queue =
      new ConcurrentLinkedQueue<>();

  private final SequenceGenerator seqGen = new SequenceGenerator();

  private final int maxCacheNumber;

  private final Semaphore cacheSlotNumber = new Semaphore(0, true);

  private final ExecutorService flusher = Executors.newSingleThreadExecutor();

  private final ReadWriter<K, F, V, T> readWriter;

  private final Path dir;

  public AppendQueue(Path dir, ReadWriter<K, F, V, T> readWriter, int maxCacheNumber)
      throws IOException {
    this.dir = dir;
    this.readWriter = readWriter;
    this.maxCacheNumber = maxCacheNumber;
    this.cacheSlotNumber.release(this.maxCacheNumber);
    reload();
    removeTempFiles();
  }

  private void removeTempFiles() throws IOException {
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, path -> path.endsWith(Constants.SUFFIX_FILE_TEMP))) {
      for (Path path : stream) {
        LOGGER.info("remove temp file {}", path);
        Files.deleteIfExists(path);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.info("no dir named {}", dir);
    }
  }

  private void reload() throws IOException {
    SortedMap<Long, Path> sortedTablePaths = getSortedTablePaths();
    for (Path path : sortedTablePaths.values()) {
      queue.add(new TableEntity<>(path, readWriter));
    }
    sortedTablePaths.keySet().stream().limit(1).forEach(seqGen::reset);
  }

  public void offer(Table<K, F, V, T> table) throws InterruptedException {
    cacheSlotNumber.acquire();

    Path tablePath =
        dir.resolve(String.format("%d%s", seqGen.next(), Constants.SUFFIX_FILE_PARQUET));
    TableEntity<K, F, V, T> entity = new TableEntity<>(tablePath, table, readWriter);
    flusher.submit(
        () -> {
          try {
            entity.flush();
          } catch (Throwable e) {
            LOGGER.error("failed to flush {}", tablePath, e);
          } finally {
            cacheSlotNumber.release();
          }
        });

    queue.add(entity);
  }

  @Nonnull
  @Override
  public Iterator<Table<K, F, V, T>> iterator() {
    return Iterators.transform(queue.iterator(), TableEntity::data);
  }

  public void clear() throws StorageException {
    try {
      cacheSlotNumber.acquire(maxCacheNumber);
      LOGGER.info("clearing data of {}", dir);
      queue.clear();
      seqGen.reset();
      MoreFiles.deleteDirectoryContents(dir, RecursiveDeleteOption.ALLOW_INSECURE);
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("delete not existed dir named {} ", dir, ignored);
    } catch (InterruptedException | IOException e) {
      throw new StorageException(e);
    } finally {
      cacheSlotNumber.release(maxCacheNumber);
    }
  }

  public void close() throws Exception {
    flusher.shutdown();
    cacheSlotNumber.acquire(maxCacheNumber);
  }

  private static class TableEntity<K extends Comparable<K>, F, V, T> {
    private final Path path;

    private final ReadWriter<K, F, V, T> readWriter;

    private final AtomicReference<Table<K, F, V, T>> toWrite;

    public TableEntity(Path path, ReadWriter<K, F, V, T> readWriter) {
      this(path, new FileTable<>(path, readWriter), readWriter);
    }

    public TableEntity(Path path, Table<K, F, V, T> toWrite, ReadWriter<K, F, V, T> readWriter) {
      this.path = path;
      this.readWriter = readWriter;
      this.toWrite = new AtomicReference<>(toWrite);
    }

    public void flush() throws IOException, StorageException {
      Table<K, F, V, T> table = toWrite.get();
      Path temp = path.resolveSibling(path.getFileName() + ".tmp");
      TableMeta<F, T> meta = table.getMeta();
      try (Scanner<K, Scanner<F, V>> scanner = table.scan(meta.getSchema().keySet(), Range.all())) {
        LOGGER.info("flushing into {}", temp);
        readWriter.flush(temp, scanner, meta.getSchema(), meta.getExtra());
        LOGGER.info("rename {} to {}", temp, path);
        Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
      }
      toWrite.set(new FileTable<>(path, readWriter));
    }

    public Table<K, F, V, T> data() {
      return toWrite.get();
    }
  }

  @Nonnull
  private SortedMap<Long, Path> getSortedTablePaths() throws IOException {
    TreeMap<Long, Path> sortedFiles = new TreeMap<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, path -> path.endsWith(Constants.SUFFIX_FILE_PARQUET))) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        long sequence = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
        sortedFiles.put(sequence, path);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.info("no dir named {}", dir);
    }
    return sortedFiles;
  }
}
