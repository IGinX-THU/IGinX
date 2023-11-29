package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.exec.RowUnionScanner;
import cn.edu.tsinghua.iginx.parquet.io.FileReaderWriter;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, V> implements Database<K, F, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OneTierDB.class);

  private final Path dir;

  private WriteBuffer<K, F, V> writeCache = new WriteBuffer<>();

  private WriteBuffer<K, F, V> flushedCache;

  private final ReadWriteLock flushingLock = new ReentrantReadWriteLock(true);

  private final ExecutorService flushPool = Executors.newSingleThreadExecutor();

  private final SequenceGenerator sequenceGenerator;

  private final LongAdder inserted = new LongAdder();

  private final FileReaderWriter<K, F, V> readerWriter;

  public OneTierDB(Path dir, FileReaderWriter<K, F, V> readerWriter) throws NativeStorageException {
    this.dir = dir;
    this.readerWriter = readerWriter;

    Set<Long> numbers = getSortedPath().keySet();
    if (!numbers.isEmpty()) {
      this.sequenceGenerator = new SequenceGenerator(numbers.iterator().next());
    } else {
      this.sequenceGenerator = new SequenceGenerator(0);
    }
  }

  @Override
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, Range<K> range)
      throws NativeStorageException {
    List<Scanner<K, Scanner<F, V>>> scanners = new ArrayList<>();
    flushingLock.readLock().lock();
    try {
      scanners.add(writeCache.scanRows(fields, range));
      if (flushedCache != null) {
        scanners.add(flushedCache.scanRows(fields, range));
      }
    } finally {
      flushingLock.readLock().unlock();
    }

    TreeMap<Long, Path> sortedFiles = getSortedPath();

    WriteBuffer<K, F, V> readerCache = new WriteBuffer<>();
    for (Path path : sortedFiles.values()) {
      try {
        readerCache.putRows(readerWriter.read(path, fields, range));
      } catch (NativeStorageException e) {
        LOG.error("failed to read file {}", path, e);
      }
    }
    scanners.add(readerCache.scanRows(fields, range));
    return new RowUnionScanner<>(scanners);
  }

  @Nonnull
  private TreeMap<Long, Path> getSortedPath() throws NativeStorageException {
    TreeMap<Long, Path> sortedFiles = new TreeMap<>();
    try (Stream<Path> paths = Files.list(dir)) {
      paths
          .filter(path -> path.toString().endsWith(".parquet"))
          .forEach(
              path -> {
                String fileName = path.getFileName().toString();
                long sequence = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                sortedFiles.put(Long.MAX_VALUE - sequence, path);
              });
    } catch (NoSuchFileException e) {
      LOG.debug("no such file or directory: {}", dir);
    } catch (IOException e) {
      throw new NativeStorageException("failed to list files", e);
    }
    return sortedFiles;
  }

  @Override
  public void upsert(Scanner<K, Scanner<F, V>> scanner) throws NativeStorageException {
    try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
        new BatchPlaneScanner<>(scanner, Constants.SIZE_INSERT_BATCH)) {
      while (batchScanner.iterate()) {
        checkSize();
        try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
          writeCache.putRows(batch);
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
        checkSize();
        try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
          writeCache.putColumns(batch);
          inserted.add(batchScanner.key());
        }
      }
    }
  }

  private void checkSize() throws NativeStorageException {
    long currentInserted = inserted.sum();
    if (currentInserted >= Constants.MAX_MEM_SIZE) {
      flushingLock.writeLock().lock();
      // TODO check size again
      try {
        flushedCache = writeCache;
        writeCache = new WriteBuffer<>();
        inserted.reset();
        flushingLock.readLock().lock();
      } finally {
        flushingLock.writeLock().unlock();
      }
      LOG.info("flushing is triggered when {} >= {}", currentInserted, Constants.MAX_MEM_SIZE);
      CountDownLatch latch = new CountDownLatch(1);
      try {
        flushPool.submit(
            () -> {
              flushingLock.readLock().lock();
              latch.countDown();
              String fileName = String.format("%019d.parquet", sequenceGenerator.getAsLong());
              Path path = this.dir.resolve(fileName);
              try {
                readerWriter.flush(path, flushedCache);
              } catch (Exception e) {
                LOG.error("failed to flush data to disk", e);
              } finally {
                flushingLock.readLock().unlock();
              }
              LOG.info("{} is flushed", path);
            });
        latch.await();
      } catch (Exception e) {
        throw new NativeStorageException(e);
      } finally {
        flushingLock.readLock().unlock();
      }
      inserted.reset();
    }
  }

  @Override
  public void delete(Set<F> fields, RangeSet<K> ranges) throws NativeStorageException {
    writeCache.remove(fields, ranges);
  }

  @Override
  public void delete(Set<F> fields) throws NativeStorageException {
    writeCache.remove(fields);
  }

  @Override
  public void delete(RangeSet<K> ranges) throws NativeStorageException {
    writeCache.remove(ranges);
  }

  @Override
  public void delete() throws NativeStorageException {
    writeCache.remove();
    FileUtils.deleteFile(dir.toFile());
  }

  @Override
  public void close() {
    String fileName = String.format("%019d.parquet", sequenceGenerator.getAsLong());
    Path path = this.dir.resolve(fileName);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("flushing is triggered when closing");
                  try {
                    readerWriter.flush(path, writeCache);
                  } catch (Exception e) {
                    LOG.error("failed to flush data to disk", e);
                  }
                }));
  }
}
