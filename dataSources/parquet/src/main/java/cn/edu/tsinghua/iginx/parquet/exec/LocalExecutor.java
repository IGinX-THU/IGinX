package cn.edu.tsinghua.iginx.parquet.exec;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.*;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.fromDuckDBDataType;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.toParquetDataType;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeTimeRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.entity.ParquetQueryRowStream;
import cn.edu.tsinghua.iginx.parquet.entity.WritePlan;
import cn.edu.tsinghua.iginx.parquet.policy.ParquetStoragePolicy;
import cn.edu.tsinghua.iginx.parquet.policy.ParquetStoragePolicy.FlushType;
import cn.edu.tsinghua.iginx.parquet.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutor implements Executor {

  private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

  // data-startKey-startPath
  private static final String DATA_FILE_NAME_FORMATTER = "data__%s__%s.parquet";

  private static final String APPENDIX_DATA_FILE_NAME_FORMATTER = "appendixData__%s__%s.parquet";

  private static final Map<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();

  private final ParquetStoragePolicy policy;

  private final Connection connection;

  public final String dataDir;

  public LocalExecutor(ParquetStoragePolicy policy, Connection connection, String dataDir) {
    this.policy = policy;
    this.connection = connection;
    this.dataDir = dataDir;
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit) {
    try {
      createDUDirectoryIfNotExists(storageUnit);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }

    if (isDummyStorageUnit) {
      return executeDummyProjectTask(paths, tagFilter, filter, storageUnit);
    }

    try {
      Connection conn = ((DuckDBConnection) connection).duplicate();
      Statement stmt = conn.createStatement();

      StringBuilder builder = new StringBuilder();
      List<String> pathList =
          determinePathListWithTagFilter(storageUnit, paths, tagFilter, isDummyStorageUnit);
      if (pathList.isEmpty()) {
        RowStream rowStream =
            new ClearEmptyRowStreamWrapper(ParquetQueryRowStream.EMPTY_PARQUET_ROW_STREAM);
        return new TaskExecuteResult(rowStream);
      }
      pathList.forEach(
          path -> builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", "));
      Path path = Paths.get(dataDir, storageUnit, "*", "*.parquet");

      ResultSet rs =
          stmt.executeQuery(
              String.format(SELECT_STMT, builder.toString(), path.toString(), filter));
      stmt.close();
      conn.close();

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new MergeTimeRowStreamWrapper(new ParquetQueryRowStream(rs, tagFilter)));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException | PhysicalException e) {
      logger.error(e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in parquet failure", e));
    }
  }

  private TaskExecuteResult executeDummyProjectTask(
      List<String> paths, TagFilter tagFilter, Filter filter, String storageUnit) {
    try {
      Connection conn = ((DuckDBConnection) connection).duplicate();
      Statement stmt = conn.createStatement();
      List<String> pathList = new ArrayList<>(paths);

      pathList = determinePathListWithTagFilter(storageUnit, pathList, tagFilter, true);
      if (pathList.isEmpty()) {
        RowStream rowStream =
            new ClearEmptyRowStreamWrapper(ParquetQueryRowStream.EMPTY_PARQUET_ROW_STREAM);
        return new TaskExecuteResult(rowStream);
      }

      StringBuilder builder = new StringBuilder();
      pathList.forEach(
          path -> builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", "));
      Path path = Paths.get(dataDir, "*.parquet");

      ResultSet rs =
          stmt.executeQuery(
              String.format(SELECT_STMT, builder.toString(), path.toString(), filter));
      stmt.close();
      conn.close();

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new MergeTimeRowStreamWrapper(new ParquetQueryRowStream(rs, tagFilter)));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException | PhysicalException e) {
      logger.error(e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in parquet failure", e));
    }
  }

  private List<String> determinePathListWithTagFilter(
      String storageUnit, List<String> patterns, TagFilter tagFilter, boolean isDummyStorageUnit)
      throws PhysicalException {
    if (tagFilter == null) {
      return determinePathList(storageUnit, patterns, isDummyStorageUnit);
    }

    List<Column> columns = new ArrayList<>();
    if (isDummyStorageUnit) {
      columns.addAll(getColumnsOfDir(Paths.get(dataDir, "*.parquet")));
    } else {
      columns.addAll(getColumnsOfStorageUnit(storageUnit));
    }

    List<String> pathList = new ArrayList<>();
    for (Column ts : columns) {
      for (String pattern : patterns) {
        if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath())
            && TagKVUtils.match(ts.getTags(), tagFilter)) {
          String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
          pathList.add(path);
          break;
        }
      }
    }
    return pathList;
  }

  private List<String> determinePathList(
      String storageUnit, List<String> patterns, boolean isDummyStorageUnit)
      throws PhysicalException {
    Set<String> patternWithoutStarSet = new HashSet<>();
    List<String> patternWithStarList = new ArrayList<>();
    patterns.forEach(
        pattern -> {
          if (pattern.contains("*")) {
            patternWithStarList.add(pattern);
          } else {
            patternWithoutStarSet.add(pattern);
          }
        });

    List<String> pathList = new ArrayList<>();
    List<Column> columns = new ArrayList<>();
    if (isDummyStorageUnit) {
      columns.addAll(getColumnsOfDir(Paths.get(dataDir, "*.parquet")));
    } else {
      columns.addAll(getColumnsOfStorageUnit(storageUnit));
    }
    for (Column ts : columns) {
      if (patternWithoutStarSet.contains(ts.getPath())) {
        String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
        pathList.add(path);
        continue;
      }
      for (String pattern : patternWithStarList) {
        if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath())) {
          String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
          pathList.add(path);
          break;
        }
      }
    }
    return pathList;
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    try {
      createDUDirectoryIfNotExists(storageUnit);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }

    DataViewWrapper data = new DataViewWrapper(dataView);
    List<WritePlan> writePlans = getWritePlans(data, storageUnit);
    for (WritePlan writePlan : writePlans) {
      ReentrantReadWriteLock lock = lockMap.get(writePlan.getFilePath().toString());
      if (lock == null) {
        lock = new ReentrantReadWriteLock();
        lockMap.putIfAbsent(writePlan.getFilePath().toString(), lock);
        lock = lockMap.get(writePlan.getFilePath().toString());
      }

      try {
        lock.writeLock().lock();
        executeWritePlan(data, writePlan);
      } catch (SQLException e) {
        logger.error("execute row write plan error", e);
        return new TaskExecuteResult(
            null, new PhysicalException("execute insert task in parquet failure", e));
      } finally {
        lock.writeLock().unlock();
      }
    }
    return new TaskExecuteResult(null, null);
  }

  private void executeWritePlan(DataViewWrapper data, WritePlan writePlan) throws SQLException {
    Connection conn = ((DuckDBConnection) connection).duplicate();
    Statement stmt = conn.createStatement();
    Path path = writePlan.getFilePath();
    String filename = writePlan.getFilePath().getFileName().toString();
    String tableName =
        filename
                .substring(0, filename.lastIndexOf("."))
                .replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)
            + System.currentTimeMillis();

    // prepare to write data.
    if (!Files.exists(path)) {
      String createTableStmt = generateCreateTableStmt(data, writePlan, tableName);
      stmt.execute(createTableStmt);
    } else {
      stmt.execute(String.format(CREATE_TABLE_FROM_PARQUET_STMT, tableName, path.toString()));
      ResultSet rs = stmt.executeQuery(String.format(DESCRIBE_STMT, tableName));
      Set<String> existsColumns = new HashSet<>();
      while (rs.next()) {
        existsColumns.add((String) rs.getObject(COLUMN_NAME));
      }
      List<String> addColumnsStmts =
          generateAddColumnsStmt(data, writePlan, tableName, existsColumns);
      if (!addColumnsStmts.isEmpty()) {
        for (String addColumnsStmt : addColumnsStmts) {
          stmt.execute(addColumnsStmt);
        }
      }
    }

    // write data
    String insertPrefix = generateInsertStmtPrefix(data, writePlan, tableName);
    String insertBody;
    switch (data.getRawDataType()) {
      case Column:
      case NonAlignedColumn:
        insertBody = generateColInsertStmtBody(data, writePlan);
        break;
      case Row:
      case NonAlignedRow:
      default:
        insertBody = generateRowInsertStmtBody(data, writePlan);
        break;
    }
    stmt.execute(insertPrefix + insertBody);

    // save to file
    stmt.execute(String.format(SAVE_TO_PARQUET_STMT, tableName, path.toString()));

    stmt.execute(String.format(DROP_TABLE_STMT, tableName));
    stmt.close();
    conn.close();
  }

  private String generateRowInsertStmtBody(DataViewWrapper data, WritePlan writePlan) {
    StringBuilder builder = new StringBuilder();

    int startPathIdx = data.getPathIndex(writePlan.getPathList().get(0));
    int endPathIdx =
        data.getPathIndex(writePlan.getPathList().get(writePlan.getPathList().size() - 1));
    int startKeyIdx = data.getKeyIndex(writePlan.getKeyInterval().getStartKey());
    int endKeyIdx = data.getKeyIndex(writePlan.getKeyInterval().getEndKey());

    for (int i = startKeyIdx; i <= endKeyIdx; i++) {
      BitmapView bitmapView = data.getBitmapView(i);
      builder.append("(").append(data.getKey(i)).append(", ");

      int index = 0;
      for (int j = 0; j < data.getPathNum(); j++) {
        if (bitmapView.get(j)) {
          if (startPathIdx <= j && j <= endPathIdx) {
            if (data.getDataType(j) == DataType.BINARY) {
              builder
                  .append("'")
                  .append(new String((byte[]) data.getValue(i, index)))
                  .append("', ");
            } else {
              builder.append(data.getValue(i, index)).append(", ");
            }
          }
          index++;
        } else {
          if (startPathIdx <= j && j <= endPathIdx) {
            builder.append("NULL, ");
          }
        }
      }
      builder.append("), ");
    }
    return builder.toString();
  }

  private String generateColInsertStmtBody(DataViewWrapper data, WritePlan writePlan) {
    int startPathIdx = data.getPathIndex(writePlan.getPathList().get(0));
    int endPathIdx =
        data.getPathIndex(writePlan.getPathList().get(writePlan.getPathList().size() - 1));
    int startKeyIdx = data.getKeyIndex(writePlan.getKeyInterval().getStartKey());
    int endKeyIdx = data.getKeyIndex(writePlan.getKeyInterval().getEndKey());

    String[] rowValueArray = new String[endKeyIdx - startKeyIdx + 1];
    for (int i = startKeyIdx; i <= endKeyIdx; i++) {
      rowValueArray[i] = "(" + data.getKey(i) + ", ";
    }
    for (int i = startPathIdx; i <= endPathIdx; i++) {
      BitmapView bitmapView = data.getBitmapView(i);

      int index = 0;
      for (int j = 0; j < data.getKeySize(); j++) {
        if (bitmapView.get(j)) {
          if (startKeyIdx <= j && j <= endKeyIdx) {
            if (data.getDataType(i) == DataType.BINARY) {
              rowValueArray[j - startKeyIdx] +=
                  "'" + new String((byte[]) data.getValue(i, index)) + "', ";
            } else {
              rowValueArray[j - startKeyIdx] += data.getValue(i, index) + ", ";
            }
          }
          index++;
        } else {
          if (startKeyIdx <= j && j <= endKeyIdx) {
            rowValueArray[j - startKeyIdx] += "NULL, ";
          }
        }
      }
    }
    for (int i = startKeyIdx; i <= endKeyIdx; i++) {
      rowValueArray[i] += "), ";
    }

    StringBuilder builder = new StringBuilder();
    for (String row : rowValueArray) {
      builder.append(row);
    }
    return builder.toString();
  }

  private List<String> generateAddColumnsStmt(
      DataViewWrapper data, WritePlan writePlan, String tableName, Set<String> existsColumns) {
    List<String> addColumnStmts = new ArrayList<>();

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i).replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
      String type = toParquetDataType(data.getDataType(i));
      if (writePlan.getPathList().contains(path.replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR))
          && !existsColumns.contains(path)) {
        builder.append("{path: ").append(path).append(", type: ").append(type).append("} ");
        addColumnStmts.add(String.format(ADD_COLUMNS_STMT, tableName, path, type));
      }
    }
    logger.info("add columns: {}", builder.toString());

    return addColumnStmts;
  }

  private String generateInsertStmtPrefix(
      DataViewWrapper data, WritePlan writePlan, String tableName) {
    StringBuilder builder = new StringBuilder();
    builder.append("time, ");
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      if (writePlan.getPathList().contains(path)) {
        builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", ");
      }
    }
    builder.deleteCharAt(builder.length() - 2);
    String columns = builder.toString();
    String insertStmtPrefix = String.format(INSERT_STMT_PREFIX, tableName, columns);
    logger.info("InsertStmtPrefix: {}", insertStmtPrefix);
    return insertStmtPrefix;
  }

  private String generateCreateTableStmt(
      DataViewWrapper data, WritePlan writePlan, String tableName) {
    StringBuilder builder = new StringBuilder();
    builder.append(COLUMN_KEY).append(" ").append(DATATYPE_BIGINT).append(", ");
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      if (writePlan.getPathList().contains(path)) {
        builder
            .append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR))
            .append(" ")
            .append(toParquetDataType(data.getDataType(i)))
            .append(", ");
      }
    }
    builder.deleteCharAt(builder.length() - 2);
    String columns = builder.toString();
    String createTableStmt = String.format(CREATE_TABLE_STMT, tableName, columns);
    logger.info("CreateTableStmt: {}", createTableStmt);
    return createTableStmt;
  }

  private List<WritePlan> getWritePlans(DataViewWrapper data, String storageUnit) {
    if (data.getKeySize() == 0) { // empty data section
      return new ArrayList<>();
    }
    KeyInterval keyInterval = new KeyInterval(data.getKey(0), data.getKey(data.getKeySize() - 1));

    Pair<Long, List<String>> latestPartition = policy.getLatestPartition(storageUnit);
    List<WritePlan> writePlans = new ArrayList<>();

    if (keyInterval.getStartKey() >= latestPartition.getK()) {
      // 所有写入数据位于最新的分区
      List<String> tsPartition = new ArrayList<>(latestPartition.getV());
      tsPartition.add(null);
      for (int i = 0; i < tsPartition.size() - 1; i++) {
        List<String> pathList = new ArrayList<>();
        String startPath = tsPartition.get(i);
        String endPath = tsPartition.get(i + 1);
        for (int j = 0; j < data.getPathNum(); j++) {
          String path = data.getPath(j);
          if (StringUtils.compare(path, startPath, true) < 0) {
            continue;
          }
          if (StringUtils.compare(path, endPath, false) >= 0) {
            break;
          }
          pathList.add(path);
        }
        if (!pathList.isEmpty()) {
          Path path =
              Paths.get(
                  dataDir,
                  storageUnit,
                  String.valueOf(latestPartition.getK()),
                  String.format(DATA_FILE_NAME_FORMATTER, latestPartition.getK(), startPath));

          long pointNum = pathList.size() * (keyInterval.getEndKey() - keyInterval.getStartKey());
          if (policy.getFlushType(path, pointNum).equals(FlushType.APPENDIX)) {
            path =
                Paths.get(
                    dataDir,
                    storageUnit,
                    String.valueOf(latestPartition.getK()),
                    String.format(
                        APPENDIX_DATA_FILE_NAME_FORMATTER, latestPartition.getK(), startPath));
          }
          writePlans.add(new WritePlan(path, pathList, keyInterval));
        }
      }
    } else {
      // 有老分区数据需要写入
      TreeMap<Long, List<String>> allPartition = policy.getAllPartition(storageUnit);
      List<Long> keyPartition = new ArrayList<>(allPartition.keySet());
      keyPartition.add(Long.MAX_VALUE);
      for (int i = 0; i < keyPartition.size() - 1; i++) {
        long startKey = keyPartition.get(i);
        long endKey = keyPartition.get(i + 1);
        KeyInterval partKeyInterval = new KeyInterval(startKey, endKey);
        if (keyInterval.isIntersect(partKeyInterval)) {
          KeyInterval keyIntersect = keyInterval.getIntersectWithLCRO(partKeyInterval);
          List<String> tsPartition = new ArrayList<>(allPartition.get(startKey));
          tsPartition.add(null);
          for (int j = 0; j < tsPartition.size() - 1; j++) {
            List<String> pathList = new ArrayList<>();
            String startPath = tsPartition.get(j);
            String endPath = tsPartition.get(j + 1);
            for (int k = 0; k < data.getPathNum(); k++) {
              String path = data.getPath(k);
              if (StringUtils.compare(path, startPath, true) < 0) {
                continue;
              }
              if (StringUtils.compare(path, endPath, false) >= 0) {
                break;
              }
              pathList.add(path);
            }
            if (!pathList.isEmpty()) {
              Path path =
                  Paths.get(
                      dataDir,
                      storageUnit,
                      String.valueOf(partKeyInterval.getStartKey()),
                      String.format(
                          DATA_FILE_NAME_FORMATTER, partKeyInterval.getStartKey(), startPath));

              long pointNum =
                  pathList.size() * (keyInterval.getEndKey() - keyInterval.getStartKey());
              if (policy.getFlushType(path, pointNum).equals(FlushType.APPENDIX)) {
                path =
                    Paths.get(
                        dataDir,
                        storageUnit,
                        String.valueOf(latestPartition.getK()),
                        String.format(
                            APPENDIX_DATA_FILE_NAME_FORMATTER, latestPartition.getK(), startPath));
              }
              writePlans.add(new WritePlan(path, pathList, keyIntersect));
            }
          }
        }
      }
    }
    return writePlans;
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    try {
      createDUDirectoryIfNotExists(storageUnit);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }

    if (keyRanges == null || keyRanges.size() == 0) { // 没有传任何 key range
      if (paths.size() == 1 && paths.get(0).equals("*") && tagFilter == null) {
        File duDir = Paths.get(dataDir, storageUnit).toFile();
        FileUtils.deleteFile(duDir);
      } else {
        List<String> deletedPaths;
        try {
          deletedPaths = determinePathListWithTagFilter(storageUnit, paths, tagFilter, false);
        } catch (PhysicalException e) {
          logger.warn("encounter error when delete path: " + e.getMessage());
          return new TaskExecuteResult(
              new PhysicalTaskExecuteFailureException(
                  "execute delete path task in parquet failure", e));
        }
        for (String path : deletedPaths) {
          deleteDataInAllFiles(storageUnit, path, null);
        }
      }
    } else {
      try {
        List<String> deletedPaths =
            determinePathListWithTagFilter(storageUnit, paths, tagFilter, false);
        for (String path : deletedPaths) {
          deleteDataInAllFiles(storageUnit, path, keyRanges);
        }
      } catch (PhysicalException e) {
        logger.error("encounter error when delete data: " + e.getMessage());
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                "execute delete data task in parquet failure", e));
      }
    }
    return new TaskExecuteResult(null, null);
  }

  private void deleteDataInAllFiles(String storageUnit, String path, List<KeyRange> keyRanges) {
    Path duDir = Paths.get(dataDir, storageUnit);
    if (Files.notExists(duDir)) {
      return;
    }
    File[] dirs = duDir.toFile().listFiles();
    if (dirs != null) {
      for (File dir : dirs) {
        File[] files = dir.listFiles();
        if (files != null) {
          for (File file : files) {
            deleteDataInFile(file, path, keyRanges);
          }
        }
      }
    }
  }

  private void deleteDataInFile(File file, String path, List<KeyRange> keyRanges) {
    path = path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
    try {
      Connection conn = ((DuckDBConnection) connection).duplicate();
      Statement stmt = conn.createStatement();
      ResultSet rs =
          stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, file.getAbsolutePath()));
      boolean hasPath = false;
      while (rs.next()) {
        String pathName = (String) rs.getObject(NAME);
        if (pathName.equals(path)) {
          hasPath = true;
          break;
        }
      }
      if (hasPath) {
        String filename = file.getName();
        String tableName =
            filename
                .substring(0, filename.lastIndexOf("."))
                .replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
        // 1.load 2.drop or delete 3.write back
        stmt.execute(
            String.format(CREATE_TABLE_FROM_PARQUET_STMT, tableName, file.getAbsolutePath()));
        if (keyRanges == null) {
          stmt.execute(String.format(DROP_COLUMN_STMT, tableName, path));
        } else {
          for (KeyRange keyRange : keyRanges) {
            stmt.execute(
                String.format(
                    DELETE_DATA_STMT,
                    tableName,
                    path,
                    keyRange.getActualBeginKey(),
                    keyRange.getActualEndKey()));
          }
        }
        stmt.execute(String.format(SAVE_TO_PARQUET_STMT, tableName, file.getAbsolutePath()));
        stmt.execute(String.format(DROP_TABLE_STMT, tableName));
      }
      stmt.close();
      conn.close();
    } catch (SQLException e) {
      logger.error("delete path failure.", e);
    }
  }

  private void createDUDirectoryIfNotExists(String storageUnit) throws PhysicalException {
    try {
      if (Files.notExists(Paths.get(dataDir, storageUnit))) {
        Files.createDirectories(Paths.get(dataDir, storageUnit));
      }
    } catch (IOException e) {
      throw new PhysicalException("fail to create du dir");
    }
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    Path path =
        Paths.get(dataDir, storageUnit, /*keyPartition*/ "*", /*pathPartition*/ "*.parquet");
    return getColumnsOfDir(path);
  }

  private List<Column> getColumnsOfDir(Path path) throws PhysicalTaskExecuteFailureException {
    Set<Column> columns = new HashSet<>();
    try {
      Connection conn = ((DuckDBConnection) connection).duplicate();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, path.toString()));
      while (rs.next()) {
        String pathName =
            ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
        Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(pathName);
        DataType type = fromDuckDBDataType((String) rs.getObject(COLUMN_TYPE));
        if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_KEY)) {
          columns.add(new Column(pair.k, type, pair.v));
        }
      }
      stmt.close();
      conn.close();
    } catch (SQLException e) {
      if (e.getMessage().contains("No files found that match the pattern")) {
        return new ArrayList<>(columns);
      }
      throw new PhysicalTaskExecuteFailureException("get columns failure", e);
    }
    return new ArrayList<>(columns);
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage() throws PhysicalException {
    File rootDir = new File(dataDir);
    List<String> parquetFiles = new ArrayList<>();
    findParquetFiles(parquetFiles, rootDir);

    long startKey = Long.MAX_VALUE, endKey = Long.MIN_VALUE;
    TreeSet<String> pathTreeSet = new TreeSet<>();
    for (String filepath : parquetFiles) {
      Pair<Set<String>, Pair<Long, Long>> ret = getBoundaryOfSingleFile(filepath);
      if (ret != null) {
        pathTreeSet.addAll(ret.getK());
        startKey = Math.min(startKey, ret.getV().getK());
        endKey = Math.max(endKey, ret.getV().getV());
      }
    }

    startKey = startKey == Long.MAX_VALUE ? 0 : startKey;
    endKey = endKey == Long.MIN_VALUE ? Long.MAX_VALUE : endKey;

    if (!pathTreeSet.isEmpty()) {
      return new Pair<>(
          new ColumnsInterval(pathTreeSet.first(), pathTreeSet.last()),
          new KeyInterval(startKey, endKey));
    } else {
      return new Pair<>(new ColumnsInterval(null, null), new KeyInterval(startKey, endKey));
    }
  }

  private Pair<Set<String>, Pair<Long, Long>> getBoundaryOfSingleFile(String filepath)
      throws PhysicalTaskExecuteFailureException {
    Path path = Paths.get(filepath);
    if (Files.notExists(path)) {
      return null;
    }

    long startKey = 0, endKey = Long.MAX_VALUE;
    Set<String> pathSet = new HashSet<>();
    try {
      Connection conn = ((DuckDBConnection) connection).duplicate();
      Statement stmt = conn.createStatement();

      ResultSet firstKeyRS =
          stmt.executeQuery(String.format(SELECT_FIRST_KEY_STMT, path.toString()));
      while (firstKeyRS.next()) {
        startKey = firstKeyRS.getLong(COLUMN_KEY);
      }
      firstKeyRS.close();

      ResultSet lastKeyRS = stmt.executeQuery(String.format(SELECT_LAST_KEY_STMT, path.toString()));
      while (lastKeyRS.next()) {
        endKey = lastKeyRS.getLong(COLUMN_KEY);
      }
      lastKeyRS.close();

      ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, path.toString()));
      while (rs.next()) {
        String pathName =
            ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
        if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_KEY)) {
          pathSet.add(pathName);
        }
      }
      rs.close();

      stmt.close();
      conn.close();
    } catch (SQLException e) {
      throw new PhysicalTaskExecuteFailureException("get boundary of file failure: " + filepath);
    }
    return new Pair<>(pathSet, new Pair<>(startKey, endKey));
  }

  private void findParquetFiles(List<String> parquetFiles, File file) {
    File[] files = file.listFiles();
    if (files == null || files.length == 0) {
      return;
    }
    for (File subFile : files) {
      if (subFile.isDirectory()) {
        findParquetFiles(parquetFiles, subFile);
      }
      if (subFile.getName().endsWith(".parquet")) {
        parquetFiles.add(subFile.getPath());
      }
    }
  }

  @Override
  public void close() throws PhysicalException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new PhysicalException(e);
    }
  }
}
