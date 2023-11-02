package cn.edu.tsinghua.iginx.parquet.exec;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.*;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.fromParquetDataType;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.toParquetDataType;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.entity.Column;
import cn.edu.tsinghua.iginx.parquet.entity.FileMeta;
import cn.edu.tsinghua.iginx.parquet.tools.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DUManager {

  private static final Logger logger = LoggerFactory.getLogger(DUManager.class);

  private final String id;

  private final String dataDir;

  private final String embeddedPrefix;

  private final Map<String, ConcurrentSkipListMap<Long, Object>> memData = new HashMap<>();

  private final boolean isDummyStorageUnit;

  private final ReentrantReadWriteLock memTableLock = new ReentrantReadWriteLock();

  private String curMemTable = "";

  private Map<String, DataType> curMemTablePathMap = new HashMap<>();

  private int curMemSize = 0;

  private long curStartTime = Long.MAX_VALUE;

  private long curEndTime = Long.MIN_VALUE;

  private final Map<String, FileMeta> fileMetaMap = new HashMap<>();

  private boolean isFlushing = false;

  private final ExecutorService flushPool = Executors.newSingleThreadExecutor();

  public DUManager(
      String id,
      String dataDir,
      boolean isDummyStorageUnit,
      String embeddedPrefix)
      throws IOException {
    this.id = id;
    this.dataDir = dataDir;
    this.isDummyStorageUnit = isDummyStorageUnit;
    this.embeddedPrefix = embeddedPrefix;

    if (!isDummyStorageUnit) {
      if (Files.exists(Paths.get(dataDir, id))) {
        recoverFromDisk();
      } else {
        Files.createDirectory(Paths.get(dataDir, id));
      }
    }
  }

  private void recoverFromDisk() throws IOException {
    File duDir = new File(Paths.get(dataDir, id).toString());
    File[] dataFiles = duDir.listFiles();
    if (dataFiles != null) {
      for (File dataFile : dataFiles) {
        String fileName = dataFile.getName();
        if (!fileName.endsWith(SUFFIX_EXTRA_FILE)) {
          continue;
        }
        String extraPath = Paths.get(dataDir, id, fileName).toString();
        String fileId = fileName.substring(0, fileName.indexOf(SUFFIX_EXTRA_FILE));
        String dataPath = Paths.get(dataDir, id, fileId + SUFFIX_PARQUET_FILE).toString();

        FileInputStream is = new FileInputStream(dataFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        long startTime = 0, endTime = Long.MAX_VALUE;
        Map<String, DataType> pathMap = new HashMap<>();
        Map<String, List<KeyRange>> deleteRanges = new HashMap<>();

        String str = null;
        while ((str = br.readLine()) != null) {
          String details = str.substring(str.indexOf(" ") + 1);
          if (str.startsWith(CMD_PATHS)) {
            String[] paths = details.split(",");
            for (int i = 0; i < paths.length; i += 2) {
              String path = paths[i];
              DataType type = DataTypeTransformer.fromStringDataType(paths[i + 1]);
              pathMap.put(path, type);
            }
          } else if (str.startsWith(CMD_KEY)) {
            String[] times = details.split(",");
            if (times.length != 2) {
              logger.error("The number of time must be two");
              continue;
            }
            startTime = Long.parseLong(times[0]);
            endTime = Long.parseLong(times[1]);
          } else if (str.startsWith(CMD_DELETE)) {
            String[] deleteInfo = details.split("#");
            if (deleteInfo.length == 1) {
              String[] paths = deleteInfo[0].split(",");
              for (String path : paths) {
                pathMap.remove(path);
                deleteRanges.remove(path);
              }
            } else if (deleteInfo.length == 2) {
              String[] paths = deleteInfo[0].split(",");
              for (String path : paths) {
                String[] times = deleteInfo[1].split(",");
                for (int i = 0; i < times.length; i += 2) {
                  long start = Long.parseLong(times[i]);
                  long end = Long.parseLong(times[i + 1]);
                  if (!deleteRanges.containsKey(path)) {
                    deleteRanges.put(path, new ArrayList<>());
                  }
                  deleteRanges.get(path).add(new KeyRange(start, end));
                }
              }
            }
          }
        }
        is.close();
        br.close();

        FileMeta meta =
            new FileMeta(extraPath, dataPath, startTime, endTime, pathMap, deleteRanges);
        fileMetaMap.put(fileId, meta);
      }
    }
  }

  public List<Column> project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws SQLException {
    if (isDummyStorageUnit) {
      return projectDummy(paths, tagFilter, filter);
    }

    Map<String, Column> dataMap = new HashMap<>();
    for (Map.Entry<String, FileMeta> entry : fileMetaMap.entrySet()) {
      FileMeta fileMeta = entry.getValue();
      List<String> filePaths = determinePathList(fileMeta.getPathMap().keySet(), paths, tagFilter);
      if (!filePaths.isEmpty()) {
        List<Column> columns =
            projectInParquet(
                filePaths,
                filter,
                fileMeta.getDataPath(),
                fileMeta.getDeleteRanges(),
                fileMeta.getEndKey());
        mergeData(dataMap, columns);
      }
    }

    List<String> memPaths = determinePathList(curMemTablePathMap.keySet(), paths, tagFilter);
    if (!memPaths.isEmpty()) {
      List<Column> columns = projectInMemTable(memPaths, filter);
      mergeData(dataMap, columns);
    }
    return new ArrayList<>(dataMap.values());
  }

  private List<Column> projectDummy(List<String> paths, TagFilter tagFilter, Filter filter)
      throws SQLException {
    Map<String, Column> dataMap = new HashMap<>();
    File file = new File(dataDir);
    File[] dataFiles = file.listFiles();
    if (dataFiles != null) {
      for (File dataFile : dataFiles) {
        if (!dataFile.getName().endsWith(SUFFIX_PARQUET_FILE)) {
          continue;
        }

        Set<String> pathsInFile = getPathsFromFile(dataFile.getPath()).keySet();
        List<String> filePaths = determinePathList(pathsInFile, paths, tagFilter);
        // dir prefix in dummy column & filter be removed
        filePaths.replaceAll(s -> s.substring(s.indexOf(".") + 1));
        if (!filePaths.isEmpty()) {
          List<Column> columns =
              projectInParquet(filePaths, filter, dataFile.getPath(), null, Long.MAX_VALUE);
          mergeData(dataMap, columns);
        }
      }
    }
    return new ArrayList<>(dataMap.values());
  }

  private List<Column> projectInMemTable(List<String> paths, Filter filter) throws SQLException {
    try {
      memTableLock.readLock().lock();
      List<Column> data = new ArrayList<>();
      for (String path : paths) {
        DataType type = curMemTablePathMap.get(path);
        if (type == null) {
          continue;
        }
        Column column = new Column(path, path, type);
        ConcurrentSkipListMap<Long, Object> columnData = memData.get(path);
        if (columnData == null) {
          continue;
        }
        column.putBatchData(columnData);
        data.add(column);
      }
      return data;
    } finally {
      memTableLock.readLock().unlock();
    }
  }

  private List<Column> projectInParquet(
      List<String> paths,
      Filter filter,
      String dataPath,
      Map<String, List<KeyRange>> deleteRanges,
      long endTime)
      throws SQLException {
    // TODO: enable load
    //        Connection conn = ((DuckDBConnection) connection).duplicate();
    //        Statement stmt = conn.createStatement();

    //        StringBuilder builder = new StringBuilder();
    //        paths.forEach(
    //                path -> builder.append(path.replaceAll(IGINX_SEPARATOR,
    // PARQUET_SEPARATOR)).append(", "));
    //        ResultSet rs =
    //                stmt.executeQuery(
    //                        String.format(
    //                                SELECT_STMT, builder.toString(), dataPath,
    // FilterTransformer.toString(filter)));
    //        stmt.close();
    //        conn.close();

    //        List<Column> data = initColumns(rs);
    List<Column> data = new ArrayList<>();
    //        rs.close();

    // deal with deleted data
    if (deleteRanges != null && !deleteRanges.isEmpty()) {
      for (Column column : data) {
        if (!deleteRanges.containsKey(column.getPathName())) {
          continue;
        }

        List<KeyRange> ranges = deleteRanges.get(column.getPathName());
        if (ranges != null) {
          ranges.forEach(
              timeRange -> {
                long start = timeRange.getActualBeginKey();
                long end =
                    Math.min(timeRange.getActualEndKey(), endTime); // optimize time > xxx case
                for (long i = start; i <= end; i++) {
                  column.removeData(i);
                }
              });
        }
      }
    }
    return data;
  }

  private List<Column> initColumns(ResultSet rs) throws SQLException {
    ResultSetMetaData rsMetaData = rs.getMetaData();
    List<Column> columns = new ArrayList<>();
    for (int i = 1; i <= rsMetaData.getColumnCount(); i++) { // start from index 1
      String physicalPath = rsMetaData.getColumnName(i);
      String pathName = physicalPath.replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
      if (i == 1 && pathName.equals(COLUMN_KEY)) {
        continue;
      }
      DataType type = fromParquetDataType(rsMetaData.getColumnTypeName(i));
      // dummy path should add dir prefix
      // won't affect deleteRange because deleteRange will be null in dummy
      if (isDummyStorageUnit) {
        pathName = embeddedPrefix + "." + pathName;
      }
      columns.add(new Column(pathName, physicalPath, type));
    }

    while (rs.next()) {
      long time = (long) rs.getObject(COLUMN_KEY);
      for (Column column : columns) {
        Object value = rs.getObject(column.getPhysicalPath());
        if (value != null) {
          if (column.getType() == DataType.BINARY) {
            column.putData(time, ((String) value).getBytes());
          } else {
            column.putData(time, value);
          }
        }
      }
    }
    return columns;
  }

  private void mergeData(Map<String, Column> dataMap, List<Column> columns) {
    for (Column column : columns) {
      if (dataMap.containsKey(column.getPathName())) {
        dataMap.get(column.getPathName()).putBatchData(column.getData());
      } else {
        dataMap.put(column.getPathName(), column);
      }
    }
  }

  private boolean isEmptyReq(DataView dataView) {
    if (dataView.getPathNum() > 0 && dataView.getKeySize() > 0) {
      return false;
    }
    return true;
  }

  public void insert(DataView dataView) throws IOException {
    if (isEmptyReq(dataView)) {
      logger.warn(String.format("Inserting empty data into %s", id));
      return;
    }
    try {
      memTableLock.writeLock().lock();

      DataViewWrapper data = new DataViewWrapper(dataView);

      if (curMemTable.equals("")) { // init mem table
        curMemTable = id + "_" + System.currentTimeMillis();
      }

      declareColumns(data);
      switch (data.getRawDataType()) {
        case Column:
        case NonAlignedColumn:
          insertColumns(data);
          break;
        case Row:
        case NonAlignedRow:
          insertRows(data);
          break;
        default:
          throw new IOException("Unknown RawDataType: " + data.getRawDataType());
      }

      if (data.getMaxKey() > curEndTime) {
        curEndTime = data.getMaxKey();
      }
      if (data.getMinKey() < curStartTime) {
        curStartTime = data.getMinKey();
      }
      // TODO: enable flush
      //            if (curMemSize > MAX_MEM_SIZE) {
      //                flush();
      //            }

    } finally {
      memTableLock.writeLock().unlock();
    }
  }

  private void declareColumns(DataViewWrapper data) throws IOException {
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      DataType type = data.getDataType(i);

      DataType oldType = curMemTablePathMap.computeIfAbsent(path, k -> type);
      if (!oldType.equals(type)) {
        throw new IOException("insert " + type + " into " + path + "(" + oldType + ")");
      }
      memData.computeIfAbsent(path, k -> new ConcurrentSkipListMap<>());
    }
  }

  private void insertRows(DataViewWrapper data) {
    List<ConcurrentSkipListMap<Long, Object>> columns = new ArrayList<>();
    List<DataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      columns.add(memData.get(path));
      dataTypes.add(curMemTablePathMap.get(path));
    }

    for (int i = 0; i < data.getKeySize(); i++) {
      Long key = data.getKey(i);
      BitmapView bitmapView = data.getBitmapView(i);
      int index = 0;
      for (int j = 0; j < data.getPathNum(); j++) {
        if (bitmapView.get(j)) {
          Object value = data.getValue(i, index);
          columns.get(j).put(key, value);
          DataType type = dataTypes.get(j);
          if (type == DataType.BINARY) {
            curMemSize += ((byte[]) value).length;
          } else {
            curMemSize += DataTypeTransformer.getDataSize(type);
          }
          index++;
        }
      }
    }
  }

  private void insertColumns(DataViewWrapper data) {
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      DataType type = curMemTablePathMap.get(path);
      ConcurrentSkipListMap<Long, Object> memColumn = memData.get(path);
      BitmapView bitmapView = data.getBitmapView(i);
      int index = 0;
      for (int j = 0; j < data.getKeySize(); j++) {
        if (!bitmapView.get(j)) {
          continue;
        }
        Long key = data.getKey(j);
        Object value = data.getValue(i, index);
        memColumn.put(key, value);
        if (type == DataType.BINARY) {
          curMemSize += ((byte[]) value).length;
        } else {
          curMemSize += DataTypeTransformer.getDataSize(type);
        }
        index++;
      }
    }
  }

  private void flush() {
    String flushMemTable = curMemTable;
    Map<String, DataType> flushMemTablePathMap = new HashMap<>(curMemTablePathMap);
    long flushStartTime = curStartTime;
    long flushEndTime = curEndTime;
    FileMeta meta = new FileMeta(flushStartTime, flushEndTime, flushMemTablePathMap);

    flushPool.submit(
        () -> {
          try {
            flushToDisk(flushMemTable, flushMemTablePathMap, flushStartTime, flushEndTime, meta);
          } catch (Exception e) {
            logger.info("flush error, details: {}", e.getMessage());
          }
        });

    curMemTablePathMap = new HashMap<>();
    curMemTable = "";
    curMemSize = 0;
    curStartTime = Long.MAX_VALUE;
    curEndTime = Long.MIN_VALUE;
  }

  public void flushBeforeExist() throws IOException {
    // TODO: enable flush
    //    if (!curMemTable.equals("")) {
    //      FileMeta meta = new FileMeta(curStartTime, curEndTime, curMemTablePathMap);
    //      flushToDisk(curMemTable, curMemTablePathMap, curStartTime, curEndTime, meta);
    //    }
  }

  private void flushToDisk(
      String table, Map<String, DataType> paths, long startTime, long endTime, FileMeta fileMeta)
      throws IOException {
    isFlushing = true;

    // TODO: enable flush
    //        Connection conn = ((DuckDBConnection) connection).duplicate();
    //        Statement stmt = conn.createStatement();

    // flush data
    Path dataPath = Paths.get(dataDir, id, String.format("%s.parquet", table));
    //    stmt.execute(String.format(SAVE_TO_PARQUET_STMT, table, dataPath.toString()));
    //    stmt.execute(String.format(DROP_TABLE_STMT, table));
    fileMeta.setDataPath(dataPath.toString());

    // flush meta
    Path extraPath = Paths.get(dataDir, id, String.format("%s.extra", table));
    StringBuilder builder = new StringBuilder();
    builder.append(CMD_PATHS).append(" ");
    paths.forEach((k, v) -> builder.append(k).append(",").append(v.toString()).append(","));
    builder.deleteCharAt(builder.length() - 1).append("\n");
    builder.append(CMD_KEY).append(" ").append(startTime).append(",").append(endTime).append("\n");

    FileWriter fw = new FileWriter(extraPath.toString());
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(builder.toString());

    bw.flush();
    bw.close();
    fw.close();

    fileMeta.setExtraPath(extraPath.toString());
    fileMetaMap.put(table, fileMeta);

    //    stmt.close();
    //    conn.close();
    isFlushing = false;
  }

  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws SQLException, IOException {
    if (paths.size() == 1
        && paths.get(0).equals("*")
        && tagFilter == null
        && (keyRanges == null || keyRanges.size() == 0)) {
      clearData();
    }

    List<String> memPaths = determinePathList(curMemTablePathMap.keySet(), paths, tagFilter);
    if (!memPaths.isEmpty()) {
      deleteDataInMemTable(memPaths, keyRanges);
    }

    for (Map.Entry<String, FileMeta> entry : fileMetaMap.entrySet()) {
      List<String> filePaths =
          determinePathList(entry.getValue().getPathMap().keySet(), paths, tagFilter);
      if (!filePaths.isEmpty()) {
        deleteDataInFile(entry.getKey(), filePaths, keyRanges);
      }
    }
  }

  private void deleteDataInMemTable(List<String> paths, List<KeyRange> keyRanges) {
    try {
      memTableLock.writeLock().lock();

      if (keyRanges == null || keyRanges.isEmpty()) {
        for (String path : paths) {
          curMemTablePathMap.remove(path);
          memData.remove(path);
        }
      } else {
        for (String path : paths) {
          ConcurrentSkipListMap<Long, Object> column = memData.get(path);
          if (column == null) {
            continue;
          }
          for (KeyRange keyRange : keyRanges) {
            ConcurrentNavigableMap<Long, Object> columnRange =
                column.subMap(
                    keyRange.getBeginKey(),
                    keyRange.isIncludeBeginKey(),
                    keyRange.getEndKey(),
                    keyRange.isIncludeEndKey());
            columnRange.clear();
          }
        }
      }
    } finally {
      memTableLock.writeLock().unlock();
    }
  }

  private void deleteDataInFile(String fileName, List<String> paths, List<KeyRange> keyRanges)
      throws IOException {
    FileMeta meta = fileMetaMap.get(fileName);
    meta.deleteData(paths, keyRanges);
  }

  private void clearData() throws SQLException {
    // drop mem table
    try {
      memTableLock.writeLock().lock();

      memData.clear();

      if (!curMemTable.equals("")) {
        //                Connection conn = ((DuckDBConnection) connection).duplicate();
        //                Statement stmt = conn.createStatement();
        //
        //                stmt.execute(String.format(DROP_TABLE_STMT, curMemTable));

        curMemTablePathMap = new HashMap<>();
        curMemTable = "";
        curMemSize = 0;
        curStartTime = Long.MAX_VALUE;
        curEndTime = Long.MIN_VALUE;

        //                stmt.close();
        //                conn.close();
      }
    } finally {
      memTableLock.writeLock().unlock();
    }

    // delete parquet files
    Path path = Paths.get(dataDir, id);
    File duDir = new File(path.toString());
    FileUtils.deleteFile(duDir);
    fileMetaMap.clear();
  }

  private List<String> determinePathList(
      Set<String> paths, List<String> patterns, TagFilter tagFilter) {
    List<String> ret = new ArrayList<>();
    for (String path : paths) {
      for (String pattern : patterns) {
        Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
        if (tagFilter == null) {
          if (Pattern.matches(StringUtils.reformatPath(pattern), pair.getK())) {
            ret.add(path);
            break;
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), pair.getK())
              && TagKVUtils.match(pair.getV(), tagFilter)) {
            ret.add(path);
            break;
          }
        }
      }
    }
    return ret;
  }

  private Map<String, DataType> getPathsFromFile(String filepath) {
    if (!filepath.contains(".parquet")) {
      return new HashMap<>();
    }
    Map<String, DataType> ret = new HashMap<>();
    // TODO : enable load
    //        try {
    //            Connection conn = ((DuckDBConnection) connection).duplicate();
    //            Statement stmt = conn.createStatement();
    //            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, filepath));
    //            while (rs.next()) {
    //                String pathName =
    //                        ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR,
    // IGINX_SEPARATOR);
    //                DataType type = fromDuckDBDataType((String) rs.getObject(COLUMN_TYPE));
    //                if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_KEY)) {
    //                    ret.put(embeddedPrefix + "." + pathName, type);
    //                }
    //            }
    //            stmt.close();
    //            conn.close();
    //        } catch (SQLException e) {
    //            logger.error("get paths failure", e);
    //            return ret;
    //        }
    return ret;
  }

  public Map<String, DataType> getPaths() {
    Map<String, DataType> ret;
    if (!isDummyStorageUnit) {
      ret = new HashMap<>(curMemTablePathMap);
      fileMetaMap.forEach((k, v) -> ret.putAll(v.getPathMap()));
    } else {
      ret = new HashMap<>();
      File file = new File(dataDir);
      File[] dataFiles = file.listFiles();
      if (dataFiles != null) {
        for (File dataFile : dataFiles) {
          if (!dataFile.getName().endsWith(SUFFIX_PARQUET_FILE)) {
            continue;
          }
          ret.putAll(getPathsFromFile(dataFile.getPath()));
        }
      }
    }
    return ret;
  }

  public KeyInterval getTimeIntervalFromFile(String filepath) {
    if (!filepath.contains(".parquet")) return null;
    // TODO: enable load
    //        Long startTime = 0L;
    //        Long endTime = Long.MAX_VALUE - 1;
    //        try {
    //            Connection conn = ((DuckDBConnection) connection).duplicate();
    //            Statement stmt = conn.createStatement();
    //            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_METADATA,
    // filepath));
    //            while (rs.next()) {
    //                String pathName =
    //                        ((String) rs.getObject(COLUMN_NAME_META))
    //                                .replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
    //                if (pathName.equals(COLUMN_KEY)) {
    //                    startTime = Long.parseLong((String) rs.getObject(COLUMN_MIN_VALUE_META));
    //                    endTime = Long.parseLong((String) rs.getObject(COLUMN_MAX_VALUE_META));
    //                }
    //            }
    //            stmt.close();
    //            conn.close();
    //        } catch (SQLException e) {
    //            logger.error("get key range from parquet file failure", e);
    //            return new KeyInterval(startTime, endTime);
    //        }
    //        return new KeyInterval(startTime, endTime);
    return new KeyInterval(Long.MAX_VALUE, Long.MIN_VALUE);
  }

  public KeyInterval getTimeInterval() {
    long start = Long.MAX_VALUE;
    long end = Long.MIN_VALUE;
    if (!isDummyStorageUnit) {
      start = curStartTime;
      end = curEndTime;
      for (FileMeta fileMeta : fileMetaMap.values()) {
        if (fileMeta.getStartKey() < start) {
          start = fileMeta.getStartKey();
        }
        if (fileMeta.getEndKey() > end) {
          end = fileMeta.getEndKey();
        }
      }
      return new KeyInterval(start, end + 1);
    } else {
      File file = new File(dataDir);
      File[] dataFiles = file.listFiles();
      if (dataFiles != null) {
        for (File dataFile : dataFiles) {
          if (!dataFile.getName().endsWith(SUFFIX_PARQUET_FILE)) {
            continue;
          }
          KeyInterval in = getTimeIntervalFromFile(dataFile.getPath());
          long min = in.getStartKey();
          long max = in.getEndKey();
          start = Math.min(start, min);
          end = Math.max(end, max);
        }
      }
      return new KeyInterval(start, end + 1);
    }
  }

  public boolean isFlushing() {
    return isFlushing;
  }
}
