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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.common.Constants;
import cn.edu.tsinghua.iginx.parquet.common.Scanner;
import cn.edu.tsinghua.iginx.parquet.common.exception.SchemaException;
import cn.edu.tsinghua.iginx.parquet.common.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.common.utils.FileUtils;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.common.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.common.RangeTombstone;
import cn.edu.tsinghua.iginx.parquet.db.common.SequenceGenerator;
import cn.edu.tsinghua.iginx.parquet.db.common.scanner.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.common.scanner.ConcatScanner;
import cn.edu.tsinghua.iginx.parquet.db.common.utils.SerializeUtils;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
  private final Path dir;
  private DataBuffer<K, F, V> writtenBuffer = new DataBuffer<>();
  private DataBuffer<K, F, V> flushedBuffer;
  private RangeTombstone<K, F> writtenDeleted = new RangeTombstone<>();
  private RangeTombstone<K, F> flushedDeleted;
  private final ConcurrentHashMap<F, T> schema = new ConcurrentHashMap<>();
  private RangeSet<K> ranges = TreeRangeSet.create();
  private final ReadWriteLock flushedLock = new ReentrantReadWriteLock(true);
  private final Semaphore flusherNumber = new Semaphore(1);
  private final ReadWriteLock deletedLock = new ReentrantReadWriteLock(true);
  private final ExecutorService flushPool = Executors.newSingleThreadExecutor();
  private final SequenceGenerator sequenceGenerator = new SequenceGenerator();
  private final LongAdder inserted = new LongAdder();
  private final ReadWriter<K, F, V, T> readerWriter;

  public OneTierDB(Path dir, ReadWriter<K, F, V, T> readerWriter) throws PhysicalException {
    this.dir = dir;
    this.readerWriter = readerWriter;

    try {
      SortedMap<Long, Path> flushed = getSortedPath();
      if (!flushed.isEmpty()) {
        Long last = flushed.lastKey();
        this.sequenceGenerator.reset(last + 1);
        reloadMetaFrom(flushed.get(last));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void reloadMetaFrom(Path path) throws IOException {
    Map.Entry<Map<F, T>, Map<String, String>> meta = readerWriter.readMeta(path);

    schema.putAll(meta.getKey());
    Map<String, String> extra = meta.getValue();
    String keyRangeStr = extra.get(Constants.KEY_RANGE_NAME);

    if (keyRangeStr == null) {
      throw new RuntimeException("key range is not found in file of " + path);
    }

    RangeSet<K> ranges =
        SerializeUtils.deserializeRangeSet(keyRangeStr, readerWriter.getKeyFormat());
    this.ranges.addAll(ranges);
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    flushedLock.readLock().lock();
    try {
      TreeMap<Long, Path> sortedFiles = getSortedPath();
      for (Path path : sortedFiles.values()) {
        Map<String, String> extra = readerWriter.readMeta(path).getValue();
        String rangeTombstoneStr = extra.get(Constants.TOMBSTONE_NAME);
        if (rangeTombstoneStr == null) {
          throw new RuntimeException("tombstone is not found in file of " + path);
        }
        RangeTombstone<K, F> rangeTombstone =
            SerializeUtils.deserializeRangeTombstone(
                rangeTombstoneStr, readerWriter.getKeyFormat(), readerWriter.getFieldFormat());
        RangeTombstone.playback(rangeTombstone, readBuffer);

        try (Scanner<K, Scanner<F, V>> scanner = readerWriter.scanData(path, fields, filter)) {
          readBuffer.putRows(scanner);
        }
      }

      if (flushedDeleted != null) {
        RangeTombstone.playback(flushedDeleted, readBuffer);
      }
      if (flushedBuffer != null) {
        for (Range<K> range : ranges.asRanges()) {
          readBuffer.putRows(flushedBuffer.scanRows(fields, range));
        }
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      flushedLock.readLock().unlock();
    }

    List<Scanner<K, Scanner<F, V>>> scanners = new ArrayList<>();
    for (Range<K> range : ranges.asRanges()) {
      scanners.add(readBuffer.scanRows(fields, range));
    }
    return new ConcatScanner<>(scanners.iterator());
  }

  @Override
  public Optional<Range<K>> range() throws StorageException {
    flushedLock.readLock().lock();
    deletedLock.readLock().lock();
    try {
      RangeSet<K> rangeSet = TreeRangeSet.create();
      rangeSet.addAll(ranges);

      RangeTombstone.playback(writtenDeleted, rangeSet);

      Optional<Range<K>> writtingRange = writtenBuffer.range();
      writtingRange.ifPresent(rangeSet::add);

      if (rangeSet.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(rangeSet.span());
      }
    } finally {
      deletedLock.readLock().unlock();
      flushedLock.readLock().unlock();
    }
  }

  @Override
  public Map<F, T> schema() throws StorageException {
    return Collections.unmodifiableMap(schema);
  }

  @Nonnull
  private TreeMap<Long, Path> getSortedPath() throws IOException {
    TreeMap<Long, Path> sortedFiles = new TreeMap<>();
    try (Stream<Path> paths = Files.list(dir)) {
      paths
          .filter(path -> path.toString().endsWith(Constants.SUFFIX_FILE_PARQUET))
          .forEach(
              path -> {
                String fileName = path.getFileName().toString();
                long sequence = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                sortedFiles.put(Long.MAX_VALUE - sequence, path);
              });
    } catch (NoSuchFileException e) {
      LOGGER.warn("fail to list {}", dir, e);
    }
    return sortedFiles;
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
      throws StorageException {

    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, Constants.SIZE_INSERT_BATCH)) {
      while (batchScanner.iterate()) {
        checkBufferSize();
        checkSchema(schema);
        deletedLock.readLock().lock();
        try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
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
        new BatchPlaneScanner<>(scanner, Constants.SIZE_INSERT_BATCH)) {
      while (batchScanner.iterate()) {
        checkBufferSize();
        checkSchema(schema);
        deletedLock.readLock().lock();
        try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
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
        throw new SchemaException(
            old.toString(), entry.getValue().toString(), entry.getKey().toString());
      }
    }
  }

  private void checkBufferSize() throws StorageException {
    if (inserted.sum() < Constants.MAX_MEM_SIZE) {
      return;
    }

    synchronized (inserted) {
      if (inserted.sum() < Constants.MAX_MEM_SIZE) {
        return;
      }

      LOGGER.info("flushing is triggered when write buffer reaching {}", Constants.MAX_MEM_SIZE);

      try {
        flusherNumber.acquire();
      } catch (InterruptedException e) {
        throw new StorageException("failed to acquire semaphore", e);
      }

      DataBuffer<K, F, V> flushedBuffer;
      RangeTombstone<K, F> flushedDeleted;
      Map<F, T> flushedschema;
      RangeSet<K> flushedRangeSet;

      flushedLock.writeLock().lock();
      deletedLock.writeLock().lock();
      try {
        this.flushedBuffer = writtenBuffer;
        this.flushedDeleted = writtenDeleted;
        this.writtenBuffer = new DataBuffer<>();
        this.writtenDeleted = new RangeTombstone<>();
        this.inserted.reset();

        RangeTombstone.playback(this.flushedDeleted, ranges);
        this.flushedBuffer.range().ifPresent(ranges::add);
        if (!ranges.isEmpty()) {
          ranges.add(ranges.span());
        }

        flushedBuffer = this.flushedBuffer;
        flushedDeleted = this.flushedDeleted;
        flushedschema = new HashMap<>(schema);
        flushedRangeSet = ImmutableRangeSet.copyOf(this.ranges);

        flushedLock.readLock().lock();
      } finally {
        deletedLock.writeLock().unlock();
        flushedLock.writeLock().unlock();
      }

      CyclicBarrier barrier = new CyclicBarrier(2);

      flushPool.submit(
          () -> {
            try {
              long seq = sequenceGenerator.getAsLong();
              Path tempPath =
                  this.dir.resolve(
                      String.format(
                          "%d%s%s",
                          seq, Constants.SUFFIX_FILE_PARQUET, Constants.SUFFIX_FILE_TEMP));
              LOGGER.info("flushing into {}", tempPath);
              flushedLock.readLock().lock();
              try {
                barrier.await();

                Map<String, String> extra = new HashMap<>();
                extra.put(
                    Constants.TOMBSTONE_NAME,
                    SerializeUtils.serialize(
                        flushedDeleted,
                        readerWriter.getKeyFormat(),
                        readerWriter.getFieldFormat()));
                extra.put(
                    Constants.KEY_RANGE_NAME,
                    SerializeUtils.serialize(flushedRangeSet, readerWriter.getKeyFormat()));
                readerWriter.flush(tempPath, flushedBuffer, flushedschema, extra);
              } catch (Exception e) {
                LOGGER.error("failed to flush {}", tempPath, e);
                try {
                  Files.delete(tempPath);
                } catch (IOException ex) {
                  LOGGER.warn("failed to delete {}", tempPath, ex);
                }
              } finally {
                flushedLock.readLock().unlock();
              }

              Path newPath =
                  this.dir.resolve(String.format("%d%s", seq, Constants.SUFFIX_FILE_PARQUET));
              flushedLock.writeLock().lock();
              try {
                LOGGER.info("rename {} to {}", tempPath, newPath);
                Files.move(tempPath, newPath);
              } catch (NoSuchFileException e) {
                LOGGER.warn("no such file or directory: {}", tempPath);
              } catch (Exception e) {
                LOGGER.error("failed to rename " + tempPath, e);
              } finally {
                if (this.flushedBuffer == flushedBuffer) {
                  this.flushedBuffer = null;
                }
                if (this.flushedDeleted == flushedDeleted) {
                  this.flushedDeleted = null;
                }
                flushedLock.writeLock().unlock();
              }
            } finally {
              flusherNumber.release();
            }
          });

      try {
        barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      } finally {
        flushedLock.readLock().unlock();
      }
    }
  }

  @Override
  public void delete(Set<F> fields, RangeSet<K> ranges) throws StorageException {
    flushedLock.readLock().lock();
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove(fields, ranges);
      writtenDeleted.delete(fields, ranges);
    } finally {
      deletedLock.writeLock().unlock();
      flushedLock.readLock().unlock();
    }
  }

  @Override
  public void delete(Set<F> fields) throws StorageException {
    flushedLock.readLock().lock();
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove(fields);
      writtenDeleted.delete(fields);
    } finally {
      deletedLock.writeLock().unlock();
      flushedLock.readLock().unlock();
    }
  }

  @Override
  public void delete(RangeSet<K> ranges) throws StorageException {
    flushedLock.readLock().lock();
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove(ranges);
      writtenDeleted.delete(ranges);
    } finally {
      deletedLock.writeLock().unlock();
      flushedLock.readLock().unlock();
    }
  }

  @Override
  public void delete() throws StorageException {
    flushedLock.writeLock().lock();
    deletedLock.writeLock().lock();
    try {
      writtenBuffer.remove();
      writtenDeleted.reset();
      flushedBuffer = null;
      flushedDeleted = null;
      FileUtils.deleteFile(dir.toFile());
      sequenceGenerator.reset(Constants.SEQUENCE_START);
    } finally {
      deletedLock.writeLock().unlock();
      flushedLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    flushedLock.writeLock().lock();
    deletedLock.writeLock().lock();
    try {
      LOGGER.info("flushing is triggered when closing");
      long seq = sequenceGenerator.getAsLong();
      Path tempPath =
          this.dir.resolve(
              String.format(
                  "%d%s%s", seq, Constants.SUFFIX_FILE_PARQUET, Constants.SUFFIX_FILE_TEMP));
      Path newPath = this.dir.resolve(String.format("%d%s", seq, Constants.SUFFIX_FILE_PARQUET));

      Map<String, String> extra = new HashMap<>();
      extra.put(
          Constants.TOMBSTONE_NAME,
          SerializeUtils.serialize(
              writtenDeleted, readerWriter.getKeyFormat(), readerWriter.getFieldFormat()));

      RangeSet<K> flushedRanges = TreeRangeSet.create(ranges);
      RangeTombstone.playback(writtenDeleted, flushedRanges);
      writtenBuffer.range().ifPresent(flushedRanges::add);
      extra.put(
          Constants.KEY_RANGE_NAME,
          SerializeUtils.serialize(flushedRanges, readerWriter.getKeyFormat()));

      readerWriter.flush(tempPath, flushedBuffer, schema, extra);
      Files.move(tempPath, newPath);
    } finally {
      deletedLock.writeLock().unlock();
      flushedLock.writeLock().unlock();
    }
  }
}
