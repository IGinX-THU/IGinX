package cn.edu.tsinghua.iginx.parquet.exec;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.ADD_COLUMNS_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CMD_DELETE;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CMD_PATHS;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CMD_TIME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TIME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CREATE_TABLE_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DATATYPE_BIGINT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DELETE_DATA_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DROP_COLUMN_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DROP_TABLE_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DUCKDB_SCHEMA;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.INSERT_STMT_PREFIX;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.MAX_MEM_SIZE;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.NAME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.PARQUET_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SAVE_TO_PARQUET_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SELECT_MEM_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SELECT_PARQUET_SCHEMA;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SELECT_STMT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SUFFIX_EXTRA_FILE;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.SUFFIX_PARQUET_FILE;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.fromParquetDataType;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.toParquetDataType;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.parquet.entity.Column;
import cn.edu.tsinghua.iginx.parquet.entity.FileMeta;
import cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.parquet.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DUManager {

    private static final Logger logger = LoggerFactory.getLogger(DUManager.class);

    private final String id;

    private final String dataDir;

    private final Connection connection;

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

    public DUManager(String id, String dataDir, Connection connection, boolean isDummyStorageUnit) throws IOException {
        this.id = id;
        this.dataDir = dataDir;
        this.connection = connection;
        this.isDummyStorageUnit = isDummyStorageUnit;

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
                Map<String, List<TimeRange>> deleteRanges = new HashMap<>();

                String str = null;
                while((str = br.readLine()) != null) {
                    String details = str.substring(str.indexOf(" ") + 1);
                    if (str.startsWith(CMD_PATHS)) {
                        String[] paths = details.split(",");
                        for (int i = 0; i < paths.length; i += 2) {
                            String path = paths[i];
                            DataType type = DataTypeTransformer.fromStringDataType(paths[i + 1]);
                            pathMap.put(path, type);
                        }
                    } else if (str.startsWith(CMD_TIME)) {
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
                                    deleteRanges.get(path).add(new TimeRange(start, end));
                                }
                            }
                        }
                    }
                }
                is.close();
                br.close();

                FileMeta meta = new FileMeta(extraPath, dataPath, startTime, endTime, pathMap, deleteRanges);
                fileMetaMap.put(fileId, meta);
            }
        }
    }

    public List<Column> project(List<String> paths, TagFilter tagFilter, String filter) throws SQLException {
        if (isDummyStorageUnit) {
            return projectDummy(paths, tagFilter, filter);
        }

        Map<String, Column> dataMap = new HashMap<>();
        for (Map.Entry<String, FileMeta> entry : fileMetaMap.entrySet()) {
            FileMeta fileMeta = entry.getValue();
            List<String> filePaths = determinePathList(fileMeta.getPathMap().keySet(), paths, tagFilter);
            if (!filePaths.isEmpty()) {
                List<Column> columns = projectInParquet(filePaths, filter, fileMeta.getDataPath(), fileMeta.getDeleteRanges(), fileMeta.getEndTime());
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

    private List<Column> projectDummy(List<String> paths, TagFilter tagFilter, String filter) throws SQLException {
        Map<String, Column> dataMap = new HashMap<>();
        File file = new File(dataDir);
        File[] dataFiles = file.listFiles();
        if (dataFiles != null) {
            for (File dataFile : dataFiles) {
                if (!dataFile.getName().endsWith(SUFFIX_PARQUET_FILE)) {
                    continue;
                }

                Set<String> pathsInFile = getPathsFromFile(dataFile.getPath());
                List<String> filePaths = determinePathList(pathsInFile, paths, tagFilter);
                if (!filePaths.isEmpty()) {
                    List<Column> columns = projectInParquet(filePaths, filter, dataFile.getPath(), null, Long.MAX_VALUE);
                    mergeData(dataMap, columns);
                }
            }
        }
        return new ArrayList<>(dataMap.values());
    }

    private List<Column> projectInMemTable(List<String> paths, String filter) throws SQLException {
        try {
            memTableLock.readLock().lock();

            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();

            StringBuilder builder = new StringBuilder();
            paths.forEach(path -> builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", "));
            ResultSet rs = stmt.executeQuery(
                String.format(SELECT_MEM_STMT,
                    builder.toString(),
                    curMemTable,
                    filter));
            stmt.close();
            conn.close();

            List<Column> data = initColumns(rs);
            rs.close();
            return data;
        } finally {
            memTableLock.readLock().unlock();
        }
    }

    private List<Column> projectInParquet(List<String> paths, String filter, String dataPath,
        Map<String, List<TimeRange>> deleteRanges, long endTime) throws SQLException {
        Connection conn = ((DuckDBConnection) connection).duplicate();
        Statement stmt = conn.createStatement();

        StringBuilder builder = new StringBuilder();
        paths.forEach(path -> builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", "));
        ResultSet rs = stmt.executeQuery(
            String.format(SELECT_STMT,
                builder.toString(),
                dataPath,
                filter));
        stmt.close();
        conn.close();

        List<Column> data = initColumns(rs);
        rs.close();

        // deal with deleted data
        if (deleteRanges != null && !deleteRanges.isEmpty()) {
            for (Column column : data) {
                if (!deleteRanges.containsKey(column.getPathName())) {
                    continue;
                }

                List<TimeRange> ranges = deleteRanges.get(column.getPathName());
                if (ranges != null) {
                    ranges.forEach(timeRange -> {
                        long start = timeRange.getActualBeginTime();
                        long end = Math.min(timeRange.getActualEndTime(), endTime);  // optimize time > xxx case
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
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {  // start from index 1
            String physicalPath = rsMetaData.getColumnName(i);
            String pathName = physicalPath.replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
            if (i == 1 && pathName.equals(COLUMN_TIME)) {
                continue;
            }
            DataType type = fromParquetDataType(rsMetaData.getColumnTypeName(i));
            columns.add(new Column(pathName, physicalPath, type));
        }

        while (rs.next()) {
            long time = (long) rs.getObject(COLUMN_TIME);
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

    public void insert(DataView dataView) throws SQLException {
        try {
            memTableLock.writeLock().lock();

            DataViewWrapper data = new DataViewWrapper(dataView);

            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();

            if (curMemTable.equals("")) {  // init mem table
                curMemTable = id + "_" + System.currentTimeMillis();

                StringBuilder builder = new StringBuilder();
                builder.append(COLUMN_TIME).append(" ").append(DATATYPE_BIGINT).append(", ");
                for (int i = 0; i < data.getPathNum(); i++) {
                    String path = data.getPath(i);
                    builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR))
                        .append(" ")
                        .append(toParquetDataType(data.getDataType(i)))
                        .append(", ");
                    curMemTablePathMap.put(path, data.getDataType(i));
                }
                builder.deleteCharAt(builder.length() - 2);
                String columns = builder.toString();

                String createTableStmt = String.format(CREATE_TABLE_STMT, curMemTable, columns);
                stmt.execute(createTableStmt);
            } else {                      // add columns if needed
                for (int i = 0; i < data.getPathNum(); i++) {
                    String path = data.getPath(i);
                    if (!curMemTablePathMap.containsKey(path)) {
                        String type = toParquetDataType(data.getDataType(i));
                        stmt.execute(String.format(ADD_COLUMNS_STMT, curMemTable,
                            path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR), type));
                        curMemTablePathMap.put(path, data.getDataType(i));
                        logger.info("add columns: ({}, {})", path, type);
                    }
                }
            }

            // write data
            String insertPrefix = generateInsertStmtPrefix(data, curMemTable);
            String insertBody;
            switch (data.getRawDataType()) {
                case Column:
                case NonAlignedColumn:
                    insertBody = generateColInsertStmtBody(data);
                    break;
                case Row:
                case NonAlignedRow:
                default:
                    insertBody = generateRowInsertStmtBody(data);
                    break;
            }
            stmt.execute(insertPrefix + insertBody);

            if (data.getMaxTime() > curEndTime) {
                curEndTime = data.getMaxTime();
            }
            if (data.getMinTime() < curStartTime) {
                curStartTime = data.getMinTime();
            }

            if (curMemSize > MAX_MEM_SIZE) {
                flush();
            }

            stmt.close();
            conn.close();
        } finally {
            memTableLock.writeLock().unlock();
        }
    }

    private String generateInsertStmtPrefix(DataViewWrapper data, String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("time, ");
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", ");
        }
        builder.deleteCharAt(builder.length() - 2);
        String columns = builder.toString();
        String insertStmtPrefix = String.format(INSERT_STMT_PREFIX, tableName, columns);
        logger.info("InsertStmtPrefix: {}", insertStmtPrefix);
        return insertStmtPrefix;
    }

    private String generateRowInsertStmtBody(DataViewWrapper data) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < data.getTimeSize(); i++) {
            BitmapView bitmapView = data.getBitmapView(i);
            builder.append("(").append(data.getTimestamp(i)).append(", ");

            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    DataType type = data.getDataType(j);
                    if (type == DataType.BINARY) {
                        byte[] bytes = (byte[]) data.getValue(i, index);
                        builder.append("'").append(new String(bytes)).append("', ");
                        curMemSize += bytes.length;
                    } else {
                        builder.append(data.getValue(i, index)).append(", ");
                        curMemSize += DataTypeTransformer.getDataSize(type);
                    }
                    index++;
                } else {
                    builder.append("NULL, ");
                }
            }
            builder.append("), ");
        }
        return builder.toString();
    }

    private String generateColInsertStmtBody(DataViewWrapper data) {
        String[] rowValueArray = new String[data.getTimeSize()];
        for (int i = 0; i < data.getTimeSize(); i++) {
            rowValueArray[i] = "(" + data.getTimestamp(i) + ", ";
        }
        for (int i = 0; i < data.getPathNum(); i++) {
            BitmapView bitmapView = data.getBitmapView(i);

            int index = 0;
            for (int j = 0; j < data.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    DataType type = data.getDataType(i);
                    if (type == DataType.BINARY) {
                        byte[] bytes = (byte[]) data.getValue(i, index);
                        rowValueArray[j] += "'" + new String(bytes) + "', ";
                        curMemSize += bytes.length;
                    } else {
                        rowValueArray[j] += data.getValue(i, index) + ", ";
                        curMemSize += DataTypeTransformer.getDataSize(type);
                    }
                    index++;
                } else {
                    rowValueArray[j] += "NULL, ";
                }
            }
        }
        for (int i = 0; i < data.getTimeSize(); i++) {
            rowValueArray[i] +=  "), ";
        }

        StringBuilder builder = new StringBuilder();
        for (String row : rowValueArray) {
            builder.append(row);
        }
        return builder.toString();
    }

    private void flush() {
        FileMeta meta = new FileMeta(curStartTime, curEndTime, curMemTablePathMap);

        flushPool.submit(() -> {
            try {
                flushToDisk(curMemTable, curMemTablePathMap, curStartTime, curEndTime, meta);
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

    public void flushBeforeExist() throws IOException, SQLException {
        if (!curMemTable.equals("")) {
            FileMeta meta = new FileMeta(curStartTime, curEndTime, curMemTablePathMap);
            flushToDisk(curMemTable, curMemTablePathMap, curStartTime, curEndTime, meta);
        }
    }

    private void flushToDisk(String table, Map<String, DataType> paths, long startTime,
        long endTime, FileMeta fileMeta) throws SQLException, IOException {
        isFlushing = true;

        Connection conn = ((DuckDBConnection) connection).duplicate();
        Statement stmt = conn.createStatement();

        // flush data
        Path dataPath = Paths.get(dataDir, id, String.format("%s.parquet", table));
        stmt.execute(String.format(SAVE_TO_PARQUET_STMT, table, dataPath.toString()));
        stmt.execute(String.format(DROP_TABLE_STMT, table));
        fileMeta.setDataPath(dataPath.toString());

        // flush meta
        Path extraPath = Paths.get(dataDir, id, String.format("%s.extra", table));
        StringBuilder builder = new StringBuilder();
        builder.append(CMD_PATHS).append(" ");
        paths.forEach((k, v) -> builder.append(k).append(",").append(v.toString()).append(","));
        builder.deleteCharAt(builder.length() - 1).append("\n");
        builder.append(CMD_TIME).append(" ").append(startTime).append(",").append(endTime).append("\n");

        FileWriter fw = new FileWriter(extraPath.toString());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(builder.toString());

        bw.flush();
        bw.close();
        fw.close();

        fileMeta.setExtraPath(extraPath.toString());
        fileMetaMap.put(table, fileMeta);

        stmt.close();
        conn.close();
        isFlushing = false;
    }

    public void delete(List<String> paths, List<TimeRange> timeRanges, TagFilter tagFilter)
        throws SQLException, IOException {
        if (paths.size() == 1 && paths.get(0).equals("*") && tagFilter == null &&
            (timeRanges == null || timeRanges.size() == 0)) {
            clearData();
        }

        List<String> memPaths = determinePathList(curMemTablePathMap.keySet(), paths, tagFilter);
        if (!memPaths.isEmpty()) {
            deleteDataInMemTable(memPaths, timeRanges);
        }

        for (Map.Entry<String, FileMeta> entry : fileMetaMap.entrySet()) {
            List<String> filePaths = determinePathList(entry.getValue().getPathMap().keySet(), paths, tagFilter);
            if (!filePaths.isEmpty()) {
                deleteDataInFile(entry.getKey(), filePaths, timeRanges);
            }
        }
    }

    private void deleteDataInMemTable(List<String> paths, List<TimeRange> timeRanges)
        throws SQLException {
        try {
            memTableLock.writeLock().lock();
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();

            if (timeRanges == null || timeRanges.size() == 0) {
                for (String path : paths) {
                    stmt.execute(String.format(DROP_COLUMN_STMT, curMemTable,
                        path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)));
                    curMemTablePathMap.remove(path);
                }
            } else {
                for (String path : paths) {
                    for (TimeRange timeRange : timeRanges) {
                        stmt.execute(String.format(DELETE_DATA_STMT, curMemTable,
                            path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR),
                            timeRange.getActualBeginTime(), timeRange.getActualEndTime()));
                    }
                }
            }

            stmt.close();
            conn.close();
        } finally {
            memTableLock.writeLock().unlock();
        }
    }

    private void deleteDataInFile(String fileName, List<String> paths, List<TimeRange> timeRanges)
        throws IOException {
        FileMeta meta = fileMetaMap.get(fileName);
        meta.deleteData(paths, timeRanges);
    }

    private void clearData() throws SQLException {
        // drop mem table
        if (!curMemTable.equals("")) {
            try {
                memTableLock.writeLock().lock();
                Connection conn = ((DuckDBConnection) connection).duplicate();
                Statement stmt = conn.createStatement();

                stmt.execute(String.format(DROP_TABLE_STMT, curMemTable));

                curMemTablePathMap = new HashMap<>();
                curMemTable = "";
                curMemSize = 0;
                curStartTime = Long.MAX_VALUE;
                curEndTime = Long.MIN_VALUE;

                stmt.close();
                conn.close();
            } finally {
                memTableLock.writeLock().unlock();
            }
        }

        // delete parquet files
        Path path = Paths.get(dataDir, id);
        File duDir = new File(path.toString());
        FileUtils.deleteFile(duDir);
        fileMetaMap.clear();
    }

    private List<String> determinePathList(Set<String> paths, List<String> patterns, TagFilter tagFilter) {
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
                    if (Pattern.matches(StringUtils.reformatPath(pattern), pair.getK()) &&
                        TagKVUtils.match(pair.getV(), tagFilter)) {
                        ret.add(path);
                        break;
                    }
                }
            }
        }
        return ret;
    }

    private Set<String> getPathsFromFile(String path) {
        Set<String> ret = new HashSet<>();
        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, path));
            while (rs.next()) {
                String pathName = ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
                if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_TIME)) {
                    ret.add(pathName);
                }
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error("get paths failure", e);
            return ret;
        }
        return ret;
    }

    public Map<String, DataType> getPaths() {
        Map<String, DataType> ret = new HashMap<>(curMemTablePathMap);
        fileMetaMap.forEach((k, v) -> ret.putAll(v.getPathMap()));
        return ret;
    }

    public TimeInterval getTimeInterval() {
        long start = curStartTime;
        long end = curEndTime;
        for (FileMeta fileMeta : fileMetaMap.values()) {
            if (fileMeta.getStartTime() < start) {
                start = fileMeta.getStartTime();
            }
            if (fileMeta.getEndTime() > end) {
                end = fileMeta.getEndTime();
            }
        }
        return new TimeInterval(start, end + 1);
    }

    public boolean isFlushing() {
        return isFlushing;
    }
}
