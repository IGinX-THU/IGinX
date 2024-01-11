package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.entity.NewQueryRowStream;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import cn.edu.tsinghua.iginx.parquet.tools.FilterRowStreamWrapper;
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

  public String dataDir;

  public String dummyDir;

  private final String embeddedPrefix;

  private final Map<String, DUManager> duManagerMap = new ConcurrentHashMap<>();

  private boolean isClosed = false;

  public NewExecutor(
      Connection connection, boolean hasData, boolean readOnly, String dataDir, String dummyDir)
      throws StorageInitializationException {
    this(connection, hasData, readOnly, dataDir, dummyDir, null);
  }

  // dirPrefix: the last layer dir name of parquet data path will be added as prefix in every data
  // column
  public NewExecutor(
      Connection connection,
      boolean hasData,
      boolean readOnly,
      String dataDir,
      String dummyDir,
      String dirPrefix)
      throws StorageInitializationException {
    testValidAndInit(hasData, readOnly, dataDir, dummyDir);

    if (dummyDir != null && !dummyDir.isEmpty() && (dirPrefix == null || dirPrefix.isEmpty())) {
      embeddedPrefix = FileUtils.getLastDirName(dummyDir);
    } else {
      embeddedPrefix = dirPrefix;
    }

    this.connection = connection;

    if (hasData) {
      try {
        recoverFromParquet();
      } catch (IOException e) {
        logger.error("Initial parquet data read error, details: {}", e.getMessage());
      }
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (PhysicalException e) {
                    logger.error("Fail to close parquet executor, details: {}", e.getMessage());
                  }
                }));
  }

  // to test whether dummy_dir is dir and create data_dir if not exits
  private void testValidAndInit(
      boolean has_data, boolean read_only, String data_dir, String dummy_dir)
      throws StorageInitializationException {

    // has_data & read_only : dummy_dir required
    // has_data : dummy_dir and dir required
    // no data : dir required
    if (has_data) {
      if (dummy_dir == null || dummy_dir.isEmpty()) {
        throw new StorageInitializationException("Data dir not provided in dummy storage.");
      }
      File dummyFile = new File(dummy_dir);
      if (dummyFile.isFile()) {
        throw new StorageInitializationException(
            String.format("Dummy dir %s is not a directory.", dummy_dir));
      }
      this.dummyDir = dummy_dir;
      if (!read_only) {
        if (data_dir == null || data_dir.isEmpty()) {
          throw new StorageInitializationException(
              "Data dir not provided in non-read-only storage.");
        }
        File dataFile = new File(data_dir);
        if (dataFile.isFile()) {
          throw new StorageInitializationException(
              String.format("Data dir %s is not a directory.", data_dir));
        }
        try {
          String dummyDirPath = new File(dummy_dir).getCanonicalPath();
          String dirPath = new File(data_dir).getCanonicalPath();
          if (dummyDirPath.equals(dirPath)) {
            throw new StorageInitializationException(
                String.format(
                    "%s can't be used as dummy dir and data dir at same time.", dummy_dir));
          }
          this.dataDir = data_dir;
          createDir(data_dir);
        } catch (IOException e) {
          throw new StorageInitializationException(
              String.format(
                  "Error reading dummy dir path %s and dir path %s: " + e.getMessage(),
                  dummy_dir,
                  data_dir));
        }
      }
    } else {
      if (data_dir == null || data_dir.isEmpty()) {
        throw new StorageInitializationException("Dir not provided in non-dummy storage.");
      }
      File dataFile = new File(data_dir);
      if (dataFile.isFile()) {
        throw new StorageInitializationException(
            String.format("Data dir %s is not a directory.", data_dir));
      }
      this.dataDir = data_dir;
      createDir(data_dir);
    }
  }

  private void createDir(String dirPath) {
    Path path = Paths.get(dirPath);
    try {
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
    } catch (IOException e) {
      logger.error(String.format("Create directory %s error: " + e.getMessage(), dirPath));
    }
  }

  private void recoverFromDisk() throws IOException {
    File file = new File(dataDir);
    File[] duDirs = file.listFiles();
    if (duDirs != null) {
      for (File duDir : duDirs) {
        if (duDir.isFile()) continue;
        DUManager duManager =
            new DUManager(duDir.getName(), dataDir, connection, false, embeddedPrefix);
        duManagerMap.put(duDir.getName(), duManager);
      }
    }
  }

  private void recoverFromParquet() throws IOException {
    DUManager duManager = new DUManager(dummyDir, dummyDir, connection, true, embeddedPrefix);
    duManagerMap.put(dummyDir, duManager);
  }

  private DUManager getDUManager(String storageUnit, boolean isDummyStorageUnit)
      throws IOException {
    DUManager duManager = duManagerMap.get(storageUnit);
    if (duManager == null) {
      duManager =
          new DUManager(
              storageUnit,
              isDummyStorageUnit ? dummyDir : dataDir,
              connection,
              isDummyStorageUnit,
              embeddedPrefix);
      duManagerMap.putIfAbsent(storageUnit, duManager);
      duManager = duManagerMap.get(storageUnit);
    }
    return duManager;
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit) {
    DUManager duManager;
    try {
      duManager = getDUManager(storageUnit, isDummyStorageUnit);
    } catch (IOException e) {
      return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
    }

    try {
      List<cn.edu.tsinghua.iginx.parquet.entity.Column> columns =
          duManager.project(paths, tagFilter, filter);
      RowStream rowStream = new ClearEmptyRowStreamWrapper(new NewQueryRowStream(columns));
      rowStream = new FilterRowStreamWrapper(rowStream, filter);
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
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    DUManager duManager;
    try {
      duManager = getDUManager(storageUnit, false);
    } catch (IOException e) {
      return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
    }

    try {
      duManager.delete(paths, keyRanges, tagFilter);
    } catch (Exception e) {
      return new TaskExecuteResult(null, new PhysicalException("Fail to delete data " + e));
    }

    return new TaskExecuteResult(null, null);
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    List<Column> ret = new ArrayList<>();
    if (storageUnit.equals("*")) {
      duManagerMap.forEach(
          (id, duManager) -> {
            duManager
                .getPaths()
                .forEach(
                    (path, type) -> {
                      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                      ret.add(new Column(pair.k, type, pair.v));
                    });
          });
    } else {
      DUManager duManager;
      try {
        duManager = getDUManager(storageUnit, false);
      } catch (IOException e) {
        throw new PhysicalException("Fail to get du manager ", e);
      }

      duManager
          .getPaths()
          .forEach(
              (path, type) -> {
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                ret.add(new Column(pair.k, type, pair.v));
              });
    }
    return ret;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage() throws PhysicalException {
    List<String> paths = new ArrayList<>();
    long start = Long.MAX_VALUE, end = Long.MIN_VALUE;
    for (DUManager duManager : duManagerMap.values()) {
      duManager
          .getPaths()
          .forEach(
              (path, type) -> {
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                paths.add(pair.k);
              });
      KeyInterval interval = duManager.getTimeInterval();
      if (interval.getStartKey() < start) {
        start = interval.getStartKey();
      }
      if (interval.getEndKey() > end) {
        end = interval.getEndKey();
      }
    }
    paths.sort(String::compareTo);
    if (paths.size() == 0) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    if (start == Long.MAX_VALUE || end == Long.MIN_VALUE) {
      throw new PhysicalTaskExecuteFailureException("time range error");
    }
    ColumnsInterval columnsInterval =
        new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
    KeyInterval keyInterval = new KeyInterval(start, end);
    return new Pair<>(columnsInterval, keyInterval);
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
