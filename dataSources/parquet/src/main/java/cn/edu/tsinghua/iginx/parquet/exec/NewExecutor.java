package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.parquet.entity.Column;
import cn.edu.tsinghua.iginx.parquet.entity.NewQueryRowStream;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(NewExecutor.class);

    private final Connection connection;

    public final String dataDir;

    private final Map<String, DUManager> duManagerMap = new ConcurrentHashMap<>();

    private boolean isClosed = false;

    public NewExecutor(Connection connection, String dataDir) {
        this.connection = connection;
        this.dataDir = dataDir;

        Path path = Paths.get(dataDir);
        try {
            if (Files.exists(path)) {
                recoverFromDisk();
            } else {
                Files.createDirectory(path);
            }
        } catch (IOException e) {
            logger.error("parquet executor init error, details: {}", e.getMessage());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (PhysicalException e) {
                logger.error("Fail to close parquet executor, details: {}", e.getMessage());
            }
        }));
    }

    private void recoverFromDisk() throws IOException {
        File file = new File(dataDir);
        File[] duDirs = file.listFiles();
        if (duDirs != null) {
            for (File duDir : duDirs) {
                DUManager duManager = new DUManager(duDir.getName(), dataDir, connection, false);
                duManagerMap.put(duDir.getName(), duManager);
            }
        }
    }

    private DUManager getDUManager(String storageUnit, boolean isDummyStorageUnit) throws IOException {
        DUManager duManager = duManagerMap.get(storageUnit);
        if (duManager == null) {
            duManager = new DUManager(storageUnit, dataDir, connection, isDummyStorageUnit);
            duManagerMap.putIfAbsent(storageUnit, duManager);
            duManager = duManagerMap.get(storageUnit);
        }
        return duManager;
    }

    @Override
    public TaskExecuteResult executeProjectTask(List<String> paths, TagFilter tagFilter,
        String filter, String storageUnit, boolean isDummyStorageUnit) {
        DUManager duManager;
        try {
            duManager = getDUManager(storageUnit, isDummyStorageUnit);
        } catch (IOException e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
        }

        try {
            List<Column> columns = duManager.project(paths, tagFilter, filter);
            RowStream rowStream = new ClearEmptyRowStreamWrapper(new NewQueryRowStream(columns));
            return new TaskExecuteResult(rowStream, null);
        } catch (SQLException e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to project data ", e));
        }
    }

    @Override
    public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
        DUManager duManager;
        try {
            duManager = getDUManager(storageUnit, false);
        } catch (IOException e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
        }

        try {
            duManager.insert(dataView);
        } catch (SQLException e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to insert data ", e));
        }

        return new TaskExecuteResult(null, null);
    }

    @Override
    public TaskExecuteResult executeDeleteTask(List<String> paths, List<TimeRange> timeRanges,
        TagFilter tagFilter, String storageUnit) {
        DUManager duManager;
        try {
            duManager = getDUManager(storageUnit, false);
        } catch (IOException e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
        }

        try {
            duManager.delete(paths, timeRanges, tagFilter);
        } catch (Exception e) {
            return new TaskExecuteResult(null, new PhysicalException("Fail to delete data ", e));
        }

        return new TaskExecuteResult(null, null);
    }

    @Override
    public List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit)
        throws PhysicalException {
        List<Timeseries> ret = new ArrayList<>();
        if (storageUnit.equals("*")) {
            duManagerMap.forEach((id, duManager) -> {
                duManager.getPaths().forEach((path, type) -> {
                    Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                    ret.add(new Timeseries(pair.k, type, pair.v));
                });
            });
        } else {
            DUManager duManager;
            try {
                duManager = getDUManager(storageUnit, false);
            } catch (IOException e) {
                throw new PhysicalException("Fail to get du manager ", e);
            }

            duManager.getPaths().forEach((path, type) -> {
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                ret.add(new Timeseries(pair.k, type, pair.v));
            });
        }
        return ret;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage() throws PhysicalException {
        List<String> paths = new ArrayList<>();
        long start = Long.MAX_VALUE, end = Long.MIN_VALUE;
        for (DUManager duManager : duManagerMap.values()) {
            duManager.getPaths().forEach((path, type) -> {
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                paths.add(pair.k);
            });
            TimeInterval interval = duManager.getTimeInterval();
            if (interval.getStartTime() < start) {
                start = interval.getStartTime();
            }
            if (interval.getEndTime() > end) {
                end = interval.getEndTime();
            }
        }
        paths.sort(String::compareTo);
        if (paths.size() == 0) {
            throw new PhysicalTaskExecuteFailureException("no data");
        }
        if (start == Long.MAX_VALUE || end == Long.MIN_VALUE) {
            throw new PhysicalTaskExecuteFailureException("time range error");
        }
        TimeSeriesRange tsRange = new TimeSeriesInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
        TimeInterval timeInterval = new TimeInterval(start, end);
        return new Pair<>(tsRange, timeInterval);
    }

    @Override
    public void close() throws PhysicalException {
        if (isClosed) {
            return;
        }

        for (DUManager duManager : duManagerMap.values()) {
            try {
                duManager.flushBeforeExist();
            } catch (Exception e) {
                throw new PhysicalException("Flush before exist error, details: {}", e);
            }
        }

        try {
            connection.close();
        } catch (SQLException e) {
            throw new PhysicalException("DuckDB connection close error", e);
        }
        isClosed = true;
    }
}
