package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
    private final Path dir;
    private DataBuffer<K, F, V> writtenBuffer = new DataBuffer<>();
    private DataBuffer<K, F, V> flushedBuffer;
    private final ReadWriteLock flushingLock = new ReentrantReadWriteLock(true);
    private final ExecutorService flushPool = Executors.newSingleThreadExecutor();
    private final SequenceGenerator sequenceGenerator;
    private final LongAdder inserted = new LongAdder();
    private final ReadWriter<K, F, V> readerWriter;

    public OneTierDB(Path dir, ReadWriter<K, F, V> readerWriter) throws NativeStorageException {
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
        DataBuffer<K, F, V> readerCache = new DataBuffer<>();

        flushingLock.readLock().lock();
        try {
            TreeMap<Long, Path> sortedFiles = getSortedPath();

            for (Path path : sortedFiles.values()) {
                try {
                    readerCache.putRows(readerWriter.read(path, fields, range));
                } catch (NativeStorageException e) {
                    LOGGER.error("failed to read file {}", path, e);
                }
            }

            if (flushedBuffer != null) {
                readerCache.putRows(flushedBuffer.scanRows(fields, range));
            }
            readerCache.putRows(writtenBuffer.scanRows(fields, range));
        } finally {
            flushingLock.readLock().unlock();
        }

        return readerCache.scanRows(fields, range);
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
        flushingLock.writeLock().lock();
        try {
            if (inserted.sum() < Constants.MAX_MEM_SIZE) {
                return;
            }

            LOGGER.info("flushing is triggered when write buffer reaching {}", Constants.MAX_MEM_SIZE);

            flushedBuffer = writtenBuffer;
            writtenBuffer = new DataBuffer<>();
            inserted.reset();
            flushingLock.readLock().lock();
        } finally {
            flushingLock.writeLock().unlock();
        }

        DataBuffer<K, F, V> flushed = this.flushedBuffer;

        try {
            CountDownLatch latch = new CountDownLatch(1);
            flushPool.submit(
                    () -> {
                        long seq = sequenceGenerator.getAsLong();

                        Path tempPath = this.dir.resolve(String.format("%d.parquet.tmp", seq));
                        try {
                            flushingLock.readLock().lock();
                            latch.countDown();

                            LOGGER.info("flushing into {}", tempPath);
                            readerWriter.flush(tempPath, flushed);
                        } catch (Exception e) {
                            LOGGER.error("failed to flush " + tempPath, e);
                        } finally {
                            flushingLock.readLock().unlock();
                        }

                        Path newPath = this.dir.resolve(String.format("%d.parquet", seq));
                        try {
                            flushingLock.writeLock().lock();

                            LOGGER.info("rename {} to {}", tempPath, newPath);
                            Files.move(tempPath, newPath);
                            if (this.flushedBuffer == flushed) {
                                this.flushedBuffer = null;
                            }
                        } catch (Exception e) {
                            LOGGER.error("failed to rename " + tempPath, e);
                        } finally {
                            flushingLock.writeLock().unlock();
                        }
                    });
            latch.await();
        } catch (InterruptedException e) {
            throw new NativeStorageException("flushing is interrupted, details: " + flushed, e);
        } finally {
            flushingLock.readLock().unlock();
        }
    }

    @Override
    public void delete(Set<F> fields, RangeSet<K> ranges) throws NativeStorageException {
        writtenBuffer.remove(fields, ranges);
    }

    @Override
    public void deleteRows(Set<F> fields) throws NativeStorageException {
        writtenBuffer.remove(fields);
    }

    @Override
    public void deleteColumns(RangeSet<K> ranges) throws NativeStorageException {
        writtenBuffer.remove(ranges);
    }

    @Override
    public void clear() throws NativeStorageException {
        writtenBuffer.remove();
        FileUtils.deleteFile(dir.toFile());
    }

    @Override
    public void close() {
        String fileName = String.format("%d.parquet", sequenceGenerator.getAsLong());
        Path path = this.dir.resolve(fileName);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOGGER.info("flushing is triggered when closing");
                                    try {
                                        readerWriter.flush(path, writtenBuffer);
                                    } catch (Exception e) {
                                        LOGGER.error("failed to flush data to disk", e);
                                    }
                                }));
    }
}
