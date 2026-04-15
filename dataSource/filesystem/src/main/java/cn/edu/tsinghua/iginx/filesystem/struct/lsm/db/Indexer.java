package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class Indexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);
    private final String name;
    private final Semaphore permits;
    private ExecutorService worker = null;

    public Indexer(String name, Semaphore permits) {
        this.name = Objects.requireNonNull(name);
        this.permits = permits;
    }

    public synchronized void start() {
        Preconditions.checkState(worker == null, "indexer %s is already started", name);
        ThreadFactory workerFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("indexer-" + name + "-worker-%d")
                        .setUncaughtExceptionHandler(this::handleUncaughtException)
                        .build();
        worker = Executors.newSingleThreadExecutor(workerFactory);
    }

    public synchronized void stop() throws InterruptedException {
        if (worker != null) {
            worker.shutdownNow();
            while (!worker.awaitTermination(1, TimeUnit.SECONDS)) {
                LOGGER.warn("executor {} did not terminate in time, continue waiting", worker);
            }
            worker = null;
        }
    }

    private void handleUncaughtException(Thread t, Throwable e) {
        LOGGER.error("indexer {} thread {} is error", name, t, e);
    }

    public synchronized void submit(Runnable runnable) {
        Preconditions.checkState(worker != null, "indexer %s is not started", name);
        worker.submit(() -> {
            try {
                permits.acquire();
                try{
                    runnable.run();
                }finally {
                    permits.release();
                }
            } catch (InterruptedException ignored) {
                LOGGER.info("indexer {} worker thread is interrupted while waiting for permits, exit", name);
            }
        });
    }
}
