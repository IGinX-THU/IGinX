package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
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
  private com.google.common.collect.RangeSet<K> flushedRangeSet =
      com.google.common.collect.TreeRangeSet.create();
  private final ReadWriteLock flushedLock = new ReentrantReadWriteLock(true);
  private final Semaphore flusherNumber = new Semaphore(1);
  private final ReadWriteLock deletedLock = new ReentrantReadWriteLock(true);
  private final ExecutorService flushPool = Executors.newSingleThreadExecutor();
  private final SequenceGenerator sequenceGenerator = new SequenceGenerator();
  private final LongAdder inserted = new LongAdder();
  private final ReadWriter<K, F, V, T> readerWriter;

  public OneTierDB(Path dir, ReadWriter<K, F, V, T> readerWriter) throws NativeStorageException {
    this.dir = dir;
    this.readerWriter = readerWriter;

    Set<Long> numbers = getSortedPath().keySet();
    if (!numbers.isEmpty()) {
      this.sequenceGenerator.reset(numbers.iterator().next());
    } else {
      this.sequenceGenerator.reset(Constants.SEQUENCE_START);
    }

    initFlushedBoundary();
  }

  private void initFlushedBoundary() throws NativeStorageException {
    for (Map.Entry<Long, Path> entry : getSortedPath().entrySet()) {
      RangeTombstone<K, F> rangeTombstone = new RangeTombstone<>();
      com.google.common.collect.Range<K> range =
          readerWriter.readMeta(entry.getValue(), new HashMap<>(), rangeTombstone);
      RangeTombstone.playback(rangeTombstone, flushedRangeSet);
      flushedRangeSet.add(range);
    }
    if (!flushedRangeSet.isEmpty()) {
      flushedRangeSet.add(flushedRangeSet.span());
    }
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, Range<K> range)
      throws NativeStorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    flushedLock.readLock().lock();
    try {
      TreeMap<Long, Path> sortedFiles = getSortedPath();

      for (Path path : sortedFiles.values()) {
        try {
          RangeTombstone<K, F> rangeTombstone = new RangeTombstone<>();
          try (Scanner<K, Scanner<F, V>> scanner =
              readerWriter.read(path, fields, range, rangeTombstone)) {
            RangeTombstone.playback(rangeTombstone, readBuffer);
            readBuffer.putRows(scanner);
          }
        } catch (NativeStorageException e) {
          LOGGER.error("failed to read file {}", path, e);
        }
      }

      if (flushedDeleted != null) {
        RangeTombstone.playback(flushedDeleted, readBuffer);
      }
      if (flushedBuffer != null) {
        readBuffer.putRows(flushedBuffer.scanRows(fields, range));
      }
      deletedLock.readLock().lock();
      try {
        RangeTombstone.playback(writtenDeleted, readBuffer);
        readBuffer.putRows(writtenBuffer.scanRows(fields, range));
      } finally {
        deletedLock.readLock().unlock();
      }
    } finally {
      flushedLock.readLock().unlock();
    }

    return readBuffer.scanRows(fields, range);
  }

  @Override
  public com.google.common.collect.RangeSet<K> ranges() throws NativeStorageException {
    flushedLock.readLock().lock();
    deletedLock.readLock().lock();
    try {
      com.google.common.collect.RangeSet<K> rangeSet =
          com.google.common.collect.TreeRangeSet.create();
      rangeSet.addAll(flushedRangeSet);
      RangeTombstone.playback(writtenDeleted, rangeSet);
      rangeSet.addAll(writtenBuffer.ranges());
      if (!rangeSet.isEmpty()) {
        return ImmutableRangeSet.of(rangeSet.span());
      } else {
        return ImmutableRangeSet.of();
      }
    } finally {
      deletedLock.readLock().unlock();
      flushedLock.readLock().unlock();
    }
  }

  @Override
  public Map<F, T> schema() throws NativeStorageException {
    TreeMap<Long, Path> sortedFiles = getSortedPath();
    Map<F, T> result = new HashMap<>();
    for (Path path : sortedFiles.values()) {
      RangeTombstone<K, F> rangeTombstone = new RangeTombstone<>();
      Map<F, T> schema = new HashMap<>();

      readerWriter.readMeta(path, schema, rangeTombstone);
      RangeTombstone.playback(rangeTombstone, result);
      for (Map.Entry<F, T> entry : schema.entrySet()) {
        T oldType = result.get(entry.getKey());
        if (oldType != null && !oldType.equals(entry.getValue())) {
          throw new NativeStorageException(
              String.format(
                  "conflict types: %s and %s of %s in %s",
                  oldType, entry.getValue(), entry.getKey(), path));
        }
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  @Nonnull
  private TreeMap<Long, Path> getSortedPath() throws NativeStorageException {
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
      LOGGER.debug("no such file or directory: {}", dir);
    } catch (IOException e) {
      throw new NativeStorageException("failed to list files", e);
    }
    return sortedFiles;
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner) throws NativeStorageException {
    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, Constants.SIZE_INSERT_BATCH)) {
      while (batchScanner.iterate()) {
        checkBeforeWriting();
        try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
          writtenBuffer.putRows(batch);
          inserted.add(batchScanner.key());
        }
      }
    }
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner) throws NativeStorageException {
    try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, Constants.SIZE_INSERT_BATCH)) {
      while (batchScanner.iterate()) {
        checkBeforeWriting();
        try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
          writtenBuffer.putColumns(batch);
          inserted.add(batchScanner.key());
        }
      }
    }
  }

  private void checkBeforeWriting() throws NativeStorageException {
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
        throw new NativeStorageException("failed to acquire semaphore", e);
      }

      DataBuffer<K, F, V> flushedBuffer;
      RangeTombstone<K, F> flushedDeleted;

      flushedLock.writeLock().lock();
      deletedLock.writeLock().lock();
      try {
        this.flushedBuffer = writtenBuffer;
        this.flushedDeleted = writtenDeleted;
        this.writtenBuffer = new DataBuffer<>();
        this.writtenDeleted = new RangeTombstone<>();
        this.inserted.reset();

        RangeTombstone.playback(this.flushedDeleted, flushedRangeSet);
        flushedRangeSet.addAll(this.flushedBuffer.ranges());
        if (!flushedRangeSet.isEmpty()) {
          flushedRangeSet.add(flushedRangeSet.span());
        }

        flushedBuffer = this.flushedBuffer;
        flushedDeleted = this.flushedDeleted;
      } finally {
        deletedLock.writeLock().unlock();
        flushedLock.writeLock().unlock();
      }

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
              try {
                readerWriter.flush(tempPath, flushedBuffer, flushedDeleted);
              } catch (Exception e) {
                LOGGER.error("failed to flush " + tempPath, e);
              }

              Path newPath = this.dir.resolve(String.format("%d.parquet", seq));
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
    }
  }

  @Override
  public void delete(Set<F> fields, RangeSet<K> ranges) throws NativeStorageException {
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
  public void deleteRows(Set<F> fields) throws NativeStorageException {
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
  public void deleteColumns(RangeSet<K> ranges) throws NativeStorageException {
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
  public void clear() throws NativeStorageException {
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
  public void close() {
    String fileName = String.format("%d.parquet", sequenceGenerator.getAsLong());
    Path path = this.dir.resolve(fileName);
    LOGGER.info("flushing is triggered when closing");
    deletedLock.readLock().lock();
    try {
      readerWriter.flush(path, writtenBuffer, writtenDeleted);
    } catch (NativeStorageException e) {
      LOGGER.error("failed to close, details:" + path + ", " + writtenDeleted, e);
    } finally {
      deletedLock.writeLock().lock();
    }
  }
}
