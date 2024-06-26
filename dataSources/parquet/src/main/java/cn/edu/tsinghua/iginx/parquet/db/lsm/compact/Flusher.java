package cn.edu.tsinghua.iginx.parquet.db.lsm.compact;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

@NotThreadSafe
public class Flusher implements NoexceptAutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

  private final String name;
  private final Shared shared;
  private final BufferAllocator allocator;
  private final MemTableQueue memTableQueue;
  private final TableStorage tableStorage;

  private ScheduledExecutorService scheduler;
  private ExecutorService dispatcher;
  private ExecutorService worker;
  private boolean running = false;

  public Flusher(
      String name,
      Shared shared,
      BufferAllocator allocator,
      MemTableQueue memTableQueue,
      TableStorage tableStorage) {
    this.name = name;
    this.shared = shared;
    this.allocator =
        allocator.newChildAllocator(allocator.getName() + "-flusher", 0, Long.MAX_VALUE);
    this.memTableQueue = memTableQueue;
    this.tableStorage = tableStorage;
    start();
  }

  @Override
  public void close() {
    stop();
    allocator.close();
  }

  public void start() {
    Preconditions.checkState(!running, "flusher is already running");

    ThreadFactory expireFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-scheduler-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(expireFactory);
    long timeout = shared.getStorageProperties().getWriteBufferTimeout().toMillis();
    if (timeout > 0) {
      LOGGER.info("flusher {} start to force flush every {} ms", name, timeout);
      this.scheduler.scheduleWithFixedDelay(
          handleException(memTableQueue::flush), timeout, timeout, TimeUnit.MILLISECONDS);
    }

    ThreadFactory flusherFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-worker-%d").build();
    this.worker = Executors.newCachedThreadPool(flusherFactory);

    ThreadFactory dispatcherFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-dispatcher-%d").build();
    this.dispatcher = Executors.newSingleThreadExecutor(dispatcherFactory);
    dispatcher.submit(handleException((this::dispatch)));

    running = true;
  }

  public void stop() {
    Preconditions.checkState(running, "flusher is not running");

    scheduler.shutdownNow();
    dispatcher.shutdownNow();
    worker.shutdownNow();
    try {
      boolean schedulerTerminated = scheduler.awaitTermination(1, TimeUnit.MINUTES);
      boolean dispatcherTerminated = dispatcher.awaitTermination(1, TimeUnit.MINUTES);
      boolean flushersTerminated = worker.awaitTermination(1, TimeUnit.MINUTES);
      if (!schedulerTerminated || !dispatcherTerminated || !flushersTerminated) {
        throw new IllegalStateException("flusher is not terminated");
      }
      running = false;
    } catch (InterruptedException e) {
      LOGGER.debug("flusher is interrupted:", e);
    }
  }

  interface InterruptibleRunnable {
    void run() throws Exception;
  }

  private Runnable handleException(InterruptibleRunnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (InterruptedException e) {
        LOGGER.debug("interrupted", e);
      } catch (Exception e) {
        LOGGER.error("unexpected error", e);
      }
    };
  }

  private void dispatch() throws InterruptedException {
    long memtableIdAtLeast = 0;
    while (!Thread.currentThread().isInterrupted()) {
      long memtableId = memTableQueue.awaitNext(memtableIdAtLeast);
      LOGGER.debug("memtable {} is ready to flush", memtableId);
      worker.submit(handleException(() -> flush(memtableId)));
      memtableIdAtLeast = memtableId + 1;
    }
    throw new InterruptedException();
  }

  private void flush(@Nonnegative long memtableId) throws InterruptedException, ExecutionException {
    List<String> tableNames;

    LOGGER.trace("acquiring permit for flushing memtable {}", memtableId);

    try (MemoryTable snapshot = memTableQueue.snapshot(memtableId, allocator)) {
      tableNames = doFlush(memtableId, snapshot);
    }

    LOGGER.debug("memtable {} is flushed to tables {}", memtableId, tableNames);

    memTableQueue.eliminate(
        memtableId,
        tombstone -> {
          for (String tableName : tableNames) {
            tableStorage.commit(tableName, tombstone);
          }
        });
    LOGGER.debug("memtable {} is eliminated", memtableId);
  }

  private List<String> doFlush(long memtableId, MemoryTable snapshot) throws InterruptedException, ExecutionException {
    List<String> tableNames = new ArrayList<>();

    List<Future<List<String>>> futures = new ArrayList<>();
    long columnNumber = 0;
    for (Field field : snapshot.getFields()) {
      columnNumber++;
      String suffix = String.valueOf(columnNumber);
      shared.getFlusherPermits().acquire();
      Future<List<String>> future = worker.submit(() -> flushSubTable(memtableId, suffix, snapshot.subTable(field)));
      futures.add(future);
    }
    LOGGER.debug("memtable {} is divided into {} sub-tables", memtableId, columnNumber);
    for (Future<List<String>> future : futures) {
      tableNames.addAll(future.get());
    }

    return tableNames;
  }

  private List<String> flushSubTable(long memtableId, String suffix, MemoryTable subTable){
    try {
      LOGGER.debug("start to flush sub-table {}-{}", memtableId, suffix);
      List<String> tableNames = tableStorage.flush(memtableId, suffix, subTable);
      LOGGER.debug("sub-table {}-{} is flushed", memtableId, suffix);
      return tableNames;
    } catch (InterruptedException e) {
      LOGGER.debug("interrupted", e);
    } catch (Exception e) {
      LOGGER.error("unexpected error", e);
    }finally {
      shared.getFlusherPermits().release();
    }
    return Collections.emptyList();
  }

}
