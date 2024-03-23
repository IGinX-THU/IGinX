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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByLazyStream extends UnaryLazyStream {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByLazyStream.class);

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private final ReentrantLock lock = new ReentrantLock();

  private static final int WORKER_NUM = config.getStreamParallelGroupByWorkerNum();

  private static final int BATCH_SIZE = 2000;

  private final GroupBy groupBy;

  private Table resultTable;

  public GroupByLazyStream(GroupBy groupBy, RowStream stream) {
    super(stream);
    this.groupBy = groupBy;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (resultTable == null) {
      cacheResult();
    }
    return resultTable.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (resultTable == null) {
      cacheResult();
    }
    return resultTable.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    return resultTable.next();
  }

  private void cacheResult() throws PhysicalException {
    List<Row> rows = new ArrayList<>();
    while (stream.hasNext() && rows.size() < config.getParallelGroupByRowsThreshold()) {
      rows.add(stream.next());
    }

    List<Row> cache;
    if (stream.hasNext()) {
      // more than threshold, use parallel cache.
      cache = parallelCache(rows);
    } else {
      cache = RowUtils.cacheGroupByResult(groupBy, new Table(stream.getHeader(), rows));
    }

    Header newHeader;
    if (cache.isEmpty()) {
      newHeader = Header.EMPTY_HEADER;
    } else {
      newHeader = cache.get(0).getHeader();
    }
    this.resultTable = new Table(newHeader, cache);
  }

  private List<Row> parallelCache(List<Row> firstPartialRows) throws PhysicalException {
    // search the required fields
    Header header = stream.getHeader();
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
    List<List<Row>> partition = new ArrayList<>();
    int partialSize = (int) Math.ceil(1.0 * firstPartialRows.size() / WORKER_NUM);
    for (int i = 0; i < WORKER_NUM; i++) {
      int end = Math.min(firstPartialRows.size(), (i + 1) * partialSize);
      List<Row> queue = new ArrayList<>(firstPartialRows.subList(i * partialSize, end));
      partition.add(queue);
    }

    Map<GroupByKey, List<Row>> groups = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(WORKER_NUM);

    for (int i = 0; i < WORKER_NUM; i++) {
      int workerIndex = i;
      pool.submit(
          () -> {
            List<Row> list = partition.get(workerIndex);
            try {
              while (true) {
                // 开启多个线程但是加速比率又比较低的情况。加锁部分执行时间可能会占大头。
                // parallel get batch rows and then calculate hash value.
                lock.lock();
                int getRowCnt = 0;
                while (getRowCnt < BATCH_SIZE && stream.hasNext()) {
                  list.add(stream.next());
                  getRowCnt++;
                }
                lock.unlock();

                if (list.isEmpty()) { // no more lines
                  break;
                }

                for (Row row : list) {
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
                      groups.putIfAbsent(key, Collections.synchronizedList(new ArrayList<>()));
                  if (rows == null) {
                    rows = groups.get(key);
                  }
                  rows.add(row);
                }
                list.clear();
              }
            } catch (PhysicalException e) {
              LOGGER.error("encounter error when parallel calculate hash: ", e);
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
      return RowUtils.applyFunc(groupBy, fields, header, groups);
    } catch (PhysicalTaskExecuteFailureException e) {
      throw new PhysicalTaskExecuteFailureException("encounter error when apply func: ", e);
    }
  }
}
