package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FaultToleranceStorageTaskRepeater {

    private static final Logger logger = LoggerFactory.getLogger(FaultToleranceStorageTaskRepeater.class);

    private final StoragePhysicalTask task;

    private final IStorageManager storageManager;

    private TaskExecuteResult result;

    private final List<Table> tableList = new ArrayList<>();

    private long lastTimestamp = 0L;

    public FaultToleranceStorageTaskRepeater(StoragePhysicalTask task, TaskExecuteResult result, IStorageManager storageManager) {
        this.task = task;
        this.result = result;
        this.storageManager = storageManager;
    }

    private void fetch() throws PhysicalException { // 抛出异常 ==> 表示执行过程中出现了错误，如果读取到了部分数据，则会更新 tableList 和 lastTimestamp
        if (result == null) {
            return;
        }
        if (result.getException() != null) {
            throw result.getException();
        }
        RowStream stream = result.getRowStream();
        if (stream == null) {
            logger.error("unexpected row stream is null in FaultToleranceStorageTaskRepeater.");
            return;
        }
        Header header = null;
        List<Row> rows = new ArrayList<>();
        try {
            header = stream.getHeader();
            while (stream.hasNext()) {
                rows.add(stream.next());
            }
        } finally {
            if (header != null && !rows.isEmpty()) {
                tableList.add(new Table(header, rows));
                lastTimestamp = rows.get(rows.size() - 1).getKey();
            }
        }
    }

    public TaskExecuteResult getFinalResult() throws PhysicalException {
        PhysicalException exception = null;
        do {
            try {
                fetch();
                exception = null;
                break; // 没有任何错误
            } catch (PhysicalException e) {
                exception = e;
                result = null;
                logger.error("exception in getFinalResult:", e);
            }

            // 走到这里说明读取过程中发生了错误，需要在生成一个 result
            while (task.hasBackup()) {
                task.backUp(lastTimestamp);
                // 提交任务执行
                try {
                    result = storageManager.getIStorage(task.getStorage()).execute(task);
                    break;
                } catch (Exception e) {
                    exception = new PhysicalException(e);
                    result = null;
                }
            }

        } while (result != null);

        if (exception != null) {
            return new TaskExecuteResult(exception);
        }

        if (tableList.size() == 0) {
            return new TaskExecuteResult(Table.ONLY_KEY_TABLE);
        }
        // 顺序合并多个 table
        Table table = tableList.get(0);
        for (int i = 1; i < tableList.size(); i++) {
            table = NaiveOperatorMemoryExecutor.getInstance().executeUnion(null, table, tableList.get(i));
        }
        return new TaskExecuteResult(table);
    }
}
