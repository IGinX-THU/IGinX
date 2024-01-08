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

package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import cn.edu.tsinghua.iginx.parquet.tools.SerializeUtils;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
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

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
    private final Path dir;
    private DataBuffer<K, F, V> writtenBuffer = new DataBuffer<>();
    private DataBuffer<K, F, V> flushedBuffer;
    private RangeTombstone<K, F> writtenDeleted = new RangeTombstone<>();
    private RangeTombstone<K, F> flushedDeleted;
    private RangeSet<K> flushedRangeSet = TreeRangeSet.create();
    private final ReadWriteLock flushedLock = new ReentrantReadWriteLock(true);
    private final Semaphore flusherNumber = new Semaphore(1);
    private final ReadWriteLock deletedLock = new ReentrantReadWriteLock(true);
    private final ExecutorService flushPool = Executors.newSingleThreadExecutor();
    private final SequenceGenerator sequenceGenerator = new SequenceGenerator();
    private final LongAdder inserted = new LongAdder();
    private final ReadWriter<K, F, V, T> readerWriter;
    private final ConcurrentHashMap<F, T> schema = new ConcurrentHashMap<>();

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
        Map<String,String> extra = meta.getValue();
        String keyRangeStr = extra.get(Constants.KEY_RANGE_NAME);

        if (keyRangeStr == null) {
            throw new RuntimeException("range is not found in " + path);
        }

        RangeTombstone<K, F> rangeTombstone = new RangeTombstone<>();
        com.google.common.collect.Range<K> range =
                readerWriter.readMeta(entry.getValue(), new HashMap<>(), rangeTombstone);

        RangeTombstone.playback(rangeTombstone, flushedRangeSet);
        flushedRangeSet.add(range);
    }

    @Override
    public Scanner<K, Scanner<F, V>> query(Set<F> fields, Range<K> range) throws StorageException {
        DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

        flushedLock.readLock().lock();
        try {
            TreeMap<Long, Path> sortedFiles = getSortedPath();

            for (Path path : sortedFiles.values()) {
                try {
                    RangeTombstone<K, F> rangeTombstone = new RangeTombstone<>();
                    try (Scanner<K, Scanner<F, V>> scanner =
                                 readerWriter.readMeta(path, fields, range, rangeTombstone)) {
                        RangeTombstone.playback(rangeTombstone, readBuffer);
                        readBuffer.putRows(scanner);
                    }
                } catch (StorageException e) {
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
    public com.google.common.collect.RangeSet<K> ranges() throws StorageException {
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
    public Map<F, T> schema() throws StorageException {
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
                    throw new StorageException(
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
            LOGGER.debug("no such file or directory: {}", dir);
        }
        return sortedFiles;
    }

    @Override
    public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
            throws StorageException {
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
    public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
            throws StorageException {
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

    private void checkBeforeWriting() throws StorageException {
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
                                readerWriter.flush(tempPath, flushedBuffer, flushedDeleted);
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
    public void close() {
        String fileName = String.format("%d.parquet", sequenceGenerator.getAsLong());
        Path path = this.dir.resolve(fileName);
        LOGGER.info("flushing is triggered when closing");
        deletedLock.readLock().lock();
        try {
            readerWriter.flush(path, writtenBuffer, writtenDeleted);
        } catch (StorageException e) {
            LOGGER.error("failed to close, details:" + path + ", " + writtenDeleted, e);
        } finally {
            deletedLock.writeLock().lock();
        }
    }
}
