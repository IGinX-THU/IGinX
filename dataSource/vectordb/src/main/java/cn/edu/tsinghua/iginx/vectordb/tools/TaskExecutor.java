package cn.edu.tsinghua.iginx.vectordb.tools;

import java.util.concurrent.*;

public class TaskExecutor {

    private static ExecutorService executorService;

    static {
        int corePoolSize = 5;
        int maximumPoolSize = 10;
        long keepAliveTime = 60L;
        TimeUnit unit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(100);
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();

        executorService = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue,
                threadFactory,
                handler
        );
    }

    public static void execute(Runnable runnable) {
        executorService.execute(runnable);
    }


}
