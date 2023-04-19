package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.GroupByKey;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByLazyStream extends UnaryLazyStream {

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final Logger logger = LoggerFactory.getLogger(GroupByLazyStream.class);

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int WORKER_NUM = 5;

    private final GroupBy groupBy;

    private List<Row> cache;

    private Header header;

    private int index = 0;

    public GroupByLazyStream(GroupBy groupBy, RowStream stream) {
        super(stream);
        this.groupBy = groupBy;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            cacheResult();
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (header == null) {
            cacheResult();
        }
        return cache != null && !cache.isEmpty() && index < cache.size();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.get(index++);
    }

    private void cacheResult() throws PhysicalException {
        List<Row> rows = new ArrayList<>();
        while (stream.hasNext() && rows.size() < config.getParallelGroupByRowsThreshold()) {
            rows.add(stream.next());
        }

        if (stream.hasNext()) {
            // more than threshold, use parallel cache.
            parallelCache(rows);
        } else {
            this.cache = RowUtils.cacheGroupByResult(groupBy, new Table(stream.getHeader(), rows));
        }

        if (cache.isEmpty()) {
            header = Header.EMPTY_HEADER;
        } else {
            header = cache.get(0).getHeader();
        }
    }

    private void parallelCache(List<Row> firstPartialRows)
            throws PhysicalTaskExecuteFailureException {
        // search the required fields
        List<String> cols = groupBy.getGroupByCols();
        int[] colIndex = new int[cols.size()];
        List<Field> fields = new ArrayList<>();

        int cur = 0;
        for (String col : cols) {
            int index = header.indexOf(col);
            if (index == -1) {
                throw new PhysicalTaskExecuteFailureException(
                        String.format("Group by col [%s] not exist.", col));
            }
            colIndex[cur++] = index;
            fields.add(header.getField(index));
        }

        // split first partial rows into workers' cache
        List<BlockingQueue<Row>> partition = new ArrayList<>();
        int partialSize = (int) Math.ceil(1.0 * firstPartialRows.size() / WORKER_NUM);
        for (int i = 0; i < WORKER_NUM; i++) {
            int end = Math.min(firstPartialRows.size(), (i + 1) * partialSize);
            BlockingQueue<Row> queue =
                    new LinkedBlockingQueue<>(firstPartialRows.subList(i * partialSize, end));
            partition.add(queue);
        }

        Map<GroupByKey, List<Row>> groups = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(WORKER_NUM);

        for (int i = 0; i < WORKER_NUM; i++) {
            int workerIndex = i;
            pool.submit(
                    () -> {
                        BlockingQueue<Row> queue = partition.get(workerIndex);
                        try {
                            while (true) {
                                // no more lines
                                lock.readLock().lock();
                                if (!stream.hasNext()) {
                                    break;
                                }
                                lock.readLock().unlock();

                                // parallel get batch rows and then calculate hash value.
                                lock.writeLock().lock();
                                int getRowSize = 0;
                                while (getRowSize < 100 && stream.hasNext()) {
                                    queue.add(stream.next());
                                    getRowSize++;
                                }
                                lock.writeLock().unlock();

                                while (!queue.isEmpty()) {
                                    Row row = queue.take();
                                    Object[] values = row.getValues();
                                    List<Object> hashValues = new ArrayList<>();
                                    for (int index : colIndex) {
                                        if (values[index] instanceof byte[]) {
                                            hashValues.add(new String((byte[]) values[index]));
                                        } else {
                                            hashValues.add(values[index]);
                                        }
                                    }

                                    GroupByKey key = new GroupByKey(hashValues);
                                    // make sure concurrent safe.
                                    List<Row> rows =
                                            groups.putIfAbsent(
                                                    key,
                                                    Collections.synchronizedList(
                                                            new ArrayList<>()));
                                    if (rows == null) {
                                        rows = groups.get(key);
                                    }
                                    rows.add(row);
                                }
                            }
                        } catch (PhysicalException | InterruptedException e) {
                            logger.error("encounter error when parallel calculate hash: ", e);
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new PhysicalTaskExecuteFailureException(
                    "encounter error when wait for parallel build: ", e);
        }

        try {
            this.cache = RowUtils.applyFunc(groupBy, fields, header, groups);
        } catch (PhysicalTaskExecuteFailureException e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when apply func: ", e);
        }
    }
}
