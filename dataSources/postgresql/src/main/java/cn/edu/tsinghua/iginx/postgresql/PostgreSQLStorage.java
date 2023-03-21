/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.postgresql;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.postgresql.query.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.*;
import static cn.edu.tsinghua.iginx.postgresql.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.toFullName;

public class PostgreSQLStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

    private final StorageEngineMeta meta;

    private final Map<String, PGConnectionPoolDataSource> connectionPoolMap = new ConcurrentHashMap<>();

    private final Connection connection;

    public PostgreSQLStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl = String
            .format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(),
                username, password);
        try {
            connection = DriverManager.getConnection(connUrl);
        } catch (SQLException e) {
            throw new StorageInitializationException("cannot connect to " + meta);
        }
    }

    private boolean testConnection() {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl = String
            .format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(),
                username, password);
        try {
            Class.forName("org.postgresql.Driver");
            DriverManager.getConnection(connUrl);
            return true;
        } catch (SQLException | ClassNotFoundException e) {
            return false;
        }
    }

    private String getUrl(String databaseName) {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl = String
            .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(), databaseName,
                username, password);
        return connUrl;
    }

    private Connection getConnection(String databaseName) {
        if (databaseName.startsWith("dummy")) {
            return null;
        }
        if (databaseName.equalsIgnoreCase("template0") || databaseName.equalsIgnoreCase("template1")) {
            return null;
        }

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format(CREATE_DATABASE_STATEMENT, databaseName));
            stmt.close();
        } catch (SQLException e) {
//            logger.info("database {} exists!", databaseName);
        }

        try {
            if (connectionPoolMap.containsKey(databaseName)) {
                return connectionPoolMap.get(databaseName).getConnection();
            }
            PGConnectionPoolDataSource connectionPool = new PGConnectionPoolDataSource();
            connectionPool.setUrl(getUrl(databaseName));
            connectionPoolMap.put(databaseName, connectionPool);
            return connectionPool.getConnection();
        } catch (SQLException e) {
            logger.error("cannot get connection for database {}: {}", databaseName, e.getMessage());
            return null;
        }
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(
                new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();
        boolean isDummyStorageUnit = task.isDummyStorageUnit();
        Connection conn = getConnection(storageUnit);
        if (!isDummyStorageUnit && conn == null) {
            return new TaskExecuteResult(
                new NonExecutablePhysicalTaskException(String.format("cannot connect to storage unit %s", storageUnit)));
        }

        if (op.getType() == OperatorType.Project) {
            Project project = (Project) op;
            Filter filter;
            if (operators.size() == 2) {
                filter = ((Select) operators.get(1)).getFilter();
            } else {
                filter = new AndFilter(Arrays
                    .asList(new KeyFilter(Op.GE, fragment.getTimeInterval().getStartTime()),
                        new KeyFilter(Op.L, fragment.getTimeInterval().getEndTime())));
            }
            return isDummyStorageUnit ? executeHistoryProjectTask(project, filter) : executeProjectTask(conn, storageUnit, project, filter);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(conn, storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(conn, storageUnit, delete);
        }
        return new TaskExecuteResult(
            new NonExecutablePhysicalTaskException("unsupported physical task in postgresql"));
    }

    @Override
    public List<Timeseries> getTimeSeries() {
        List<Timeseries> timeseries = new ArrayList<>();
        Map<String, String> extraParams = meta.getExtraParams();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
            while (databaseSet.next()) {
                try {
                    String databaseName = databaseSet.getString("DATNAME"); // 获取数据库名称
                    if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false")) && !databaseName.startsWith(DATABASE_PREFIX)) {
                        continue;
                    }
                    Connection conn = getConnection(databaseName);
                    if (conn == null) {
                        continue;
                    }
                    DatabaseMetaData databaseMetaData = conn.getMetaData();
                    ResultSet tableSet = databaseMetaData.getTables(databaseName, "public", "%", new String[]{"TABLE"});
                    while (tableSet.next()) {
                        String tableName = tableSet.getString("TABLE_NAME"); // 获取表名称
                        ResultSet columnSet = databaseMetaData.getColumns(databaseName, "public", tableName, "%");
                        while (columnSet.next()) {
                            String columnName = columnSet.getString("COLUMN_NAME"); // 获取列名称
                            String typeName = columnSet.getString("TYPE_NAME"); // 列字段类型
                            if (columnName.equals("time")) { // time 列不显示
                                continue;
                            }
                            Pair<String, Map<String, String>> nameAndTags = splitFullName(columnName);
                            if (databaseName.startsWith(DATABASE_PREFIX)) {
                                timeseries.add(new Timeseries(
                                    tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                    + IGINX_SEPARATOR + nameAndTags.k,
                                    DataTypeTransformer.fromPostgreSQL(typeName),
                                    nameAndTags.v)
                                );
                            } else {
                                timeseries.add(new Timeseries(
                                    databaseName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                    + IGINX_SEPARATOR + tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                    + IGINX_SEPARATOR + nameAndTags.k,
                                    DataTypeTransformer.fromPostgreSQL(typeName),
                                    nameAndTags.v)
                                );
                            }
                        }
                        columnSet.close();
                    }
                    tableSet.close();
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                }
            }
            databaseSet.close();
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return timeseries;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        long minTime = Long.MAX_VALUE, maxTime = 0;
        List<String> paths = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
            while (databaseSet.next()) {
                String databaseName = databaseSet.getString("DATNAME"); // 获取数据库名称
                Connection conn = getConnection(databaseName);
                if (conn == null) {
                    continue;
                }
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = databaseMetaData.getTables(databaseName, "public", "%", new String[]{"TABLE"});
                while (tableSet.next()) {
                    String tableName = tableSet.getString("TABLE_NAME"); // 获取表名称
                    ResultSet columnSet = databaseMetaData.getColumns(databaseName, "public", tableName, "%");
                    StringBuilder columnNames = new StringBuilder();
                    while (columnSet.next()) {
                        String columnName = columnSet.getString("COLUMN_NAME"); // 获取列名称
                        paths.add(databaseName + IGINX_SEPARATOR + tableName + IGINX_SEPARATOR + columnName);
                        columnNames.append(columnName);
                        columnNames.append(", "); // c1, c2, c3,
                    }
                    columnNames = new StringBuilder(columnNames.substring(0, columnNames.length() - 2)); // c1, c2, c3

                    // 获取 key 的范围
                    String statement = String.format(CONCAT_QUERY_STATEMENT, columnNames, tableName);
                    Statement concatStmt = conn.createStatement();
                    ResultSet concatSet = concatStmt.executeQuery(statement);
                    while (concatSet.next()) {
                        String concatValue = concatSet.getString("concat");
                        long time = toHash(concatValue);
                        minTime = Math.min(time, minTime);
                        maxTime = Math.max(time, maxTime);
                    }
                }
            }
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        paths.sort(String::compareTo);
        return new Pair<>(new TimeSeriesInterval(paths.get(0), paths.get(paths.size() - 1)),
            new TimeInterval(minTime, maxTime + 1));
    }

    private Map<String, String> splitAndMergeQueryPatterns(String databaseName, Connection conn, List<String> patterns) throws SQLException {
        // table name -> column names
        // 1 -> n
        Map<String, String> tableNameToColumnNames = new HashMap<>();
        String tableName;
        String columnNames;

        for (String pattern : patterns) {
            if (pattern.equals("*") || pattern.equals("*.*")) {
                tableName = "%";
                columnNames = "%";
            } else {
                if (pattern.split("\\" + IGINX_SEPARATOR).length == 1) { // REST 查询的路径中可能不含 .
                    tableName = pattern;
                    columnNames = "%";
                } else {
                    tableName = pattern.substring(0, pattern.lastIndexOf(".")).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    columnNames = pattern.substring(pattern.lastIndexOf(".") + 1);
                    boolean columnEqualsStar = columnNames.equals("*");
                    boolean tableContainsStar = tableName.contains("*");
                    if (columnEqualsStar || tableContainsStar) {
                        tableName = tableName.replace('*', '%');
                        if (columnEqualsStar) {
                            columnNames = "%";
                            if (!tableName.endsWith("%")) {
                                tableName += "%";
                            }
                        }
                    }
                }
            }

            if (!columnNames.endsWith("%")) {
                columnNames += "%"; // 匹配 tagKV
            }
            ResultSet rs = conn.getMetaData().getColumns(databaseName, "public", tableName, columnNames);
            while (rs.next()) {
                tableName = rs.getString("TABLE_NAME");
                columnNames = rs.getString("COLUMN_NAME");
                if (columnNames.equals("time")) {
                    continue;
                }
                if (tableNameToColumnNames.containsKey(tableName)) {
                    columnNames = tableNameToColumnNames.get(tableName) + ", " + columnNames;
                }
                tableNameToColumnNames.put(tableName, columnNames);
            }
            rs.close();
        }

        return tableNameToColumnNames;
    }

    private TaskExecuteResult executeProjectTask(Connection conn, String databaseName, Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            ResultSet rs;
            Statement stmt;

            Map<String, String> tableNameToColumnNames = splitAndMergeQueryPatterns(databaseName, conn, project.getPatterns());
            for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
                String tableName = entry.getKey();
                String columnNames = Arrays.stream(entry.getValue().split(", ")).map(this::getCompleteName).reduce((a, b) -> a + ", " + b).orElse("%");
                String statement = String.format(QUERY_STATEMENT, columnNames, getCompleteName(tableName), FilterTransformer.toString(filter));
                try {
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(statement);
                    logger.info("[Query] execute query: {}", statement);
                } catch (SQLException e) {
                    logger.error("meet error when executing query {}: {}", statement, e.getMessage());
                    continue;
                }
                resultSets.add(rs);
            }

            RowStream rowStream = new ClearEmptyRowStreamWrapper(
                new PostgreSQLQueryRowStream(resultSets, false, filter, project.getTagFilter()));
            conn.close();
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
        }
    }

    private Map<String, Map<String, String>> splitAndMergeHistoryQueryPatterns(List<String> patterns) throws SQLException {
        // <database name, <table name, column names>>
        Map<String, Map<String, String>> splitResults = new HashMap<>();
        String databaseName;
        String tableName;
        String columnNames;

        for (String pattern : patterns) {
            if (pattern.equals("*") || pattern.equals("*.*")) {
                databaseName = "%";
                tableName = "%";
                columnNames = "%";
            } else {
                String[] parts = pattern.split("\\" + IGINX_SEPARATOR);
                if (parts.length > 3) { // 大于三级，不合法
                    logger.error("wrong pattern: {}", pattern);
                    continue;
                }
                if (parts.length == 2 && !parts[1].equals("*")) { // 只有两级且第二级不为 *，则此 pattern 不合法，无法拆分出三级
                    continue;
                }

                databaseName = parts[0];
                tableName = parts[1].equals("*") ? "%" : parts[1];
                columnNames = parts[2].equals("*") ? "%" : parts[2];
            }

            if (databaseName.equals("%")) {
                Statement stmt = connection.createStatement();
                ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
                while (databaseSet.next()) {
                    String tempDatabaseName = databaseSet.getString("DATNAME");
                    if (tempDatabaseName.startsWith(DATABASE_PREFIX)) {
                        continue;
                    }
                    Connection conn = getConnection(tempDatabaseName);
                    if (conn == null) {
                        continue;
                    }
                    DatabaseMetaData databaseMetaData = conn.getMetaData();
                    ResultSet tableSet = databaseMetaData.getTables(tempDatabaseName, "public", tableName, new String[]{"TABLE"});
                    while (tableSet.next()) {
                        String tempTableName = tableSet.getString("TABLE_NAME");
                        ResultSet columnSet = databaseMetaData.getColumns(tempDatabaseName, "public", tempTableName, columnNames);
                        while (columnSet.next()) {
                            String tempColumnNames = columnSet.getString("COLUMN_NAME");
                            Map<String, String> tableNameToColumnNames = new HashMap<>();
                            if (splitResults.containsKey(tempDatabaseName)) {
                                tableNameToColumnNames = splitResults.get(tempDatabaseName);
                                tempColumnNames = tableNameToColumnNames.get(tempTableName) + ", " + tempColumnNames;
                            }
                            tableNameToColumnNames.put(tempTableName, tempColumnNames);
                            splitResults.put(tempDatabaseName, tableNameToColumnNames);
                        }
                    }
                }
            } else {
                Connection conn = getConnection(databaseName);
                if (conn == null) {
                    continue;
                }
                ResultSet rs = conn.getMetaData().getColumns(databaseName, "public", tableName, columnNames);
                while (rs.next()) {
                    tableName = rs.getString("TABLE_NAME");
                    columnNames = rs.getString("COLUMN_NAME");
                    Map<String, String> tableNameToColumnNames = new HashMap<>();
                    if (splitResults.containsKey(databaseName)) {
                        tableNameToColumnNames = splitResults.get(databaseName);
                        columnNames = tableNameToColumnNames.get(tableName) + ", " + columnNames;
                    }
                    tableNameToColumnNames.put(tableName, columnNames);
                    splitResults.put(databaseName, tableNameToColumnNames);
                }
            }
        }

        return splitResults;
    }

    private TaskExecuteResult executeHistoryProjectTask(Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            ResultSet rs;
            Connection conn = null;
            Statement stmt;

            Map<String, Map<String, String>> splitResults = splitAndMergeHistoryQueryPatterns(project.getPatterns());
            for (Map.Entry<String, Map<String, String>> splitEntry : splitResults.entrySet()) {
                String databaseName = splitEntry.getKey();
                conn = getConnection(databaseName);
                if (conn == null) {
                    continue;
                }
                for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
                    String tableName = entry.getKey();
                    String columnNames = Arrays.stream(entry.getValue().split(", ")).map(this::getCompleteName).reduce((a, b) -> a + ", " + b).orElse("%");
                    String statement = String.format(QUERY_STATEMENT_WITHOUT_WHERE_CLAUSE, columnNames, columnNames, getCompleteName(tableName));
                    try {
                        stmt = conn.createStatement();
                        rs = stmt.executeQuery(statement);
                        logger.info("[Query] execute query: {}", statement);
                    } catch (SQLException e) {
                        logger.error("meet error when executing query {}: {}", statement, e.getMessage());
                        continue;
                    }
                    resultSets.add(rs);
                }
            }

            RowStream rowStream = new ClearEmptyRowStreamWrapper(
                new PostgreSQLQueryRowStream(resultSets, true, filter, project.getTagFilter()));
            if (conn != null) {
                conn.close();
            }
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
        }
    }

    private TaskExecuteResult executeInsertTask(Connection conn, String storageUnit, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertNonAlignedRowRecords(conn, storageUnit, (RowDataView) dataView);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords(conn, storageUnit, (ColumnDataView) dataView);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null,
                new PhysicalException("execute insert task in postgresql failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private void createOrAlterTables(Connection conn, String storageUnit, List<String> paths, List<Map<String, String>> tagsList, List<DataType> dataTypeList) {
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            Map<String, String> tags = new HashMap<>();
            if (tagsList != null && !tagsList.isEmpty()) {
                tags = tagsList.get(i);
            }
            DataType dataType = dataTypeList.get(i);
            String tableName = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            String columnName = path.substring(path.lastIndexOf('.') + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);

            try {
                Statement stmt = conn.createStatement();
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = databaseMetaData.getTables(storageUnit, "public", tableName, new String[]{"TABLE"});
                columnName = toFullName(columnName, tags);
                if (!tableSet.next()) {
                    String statement = String.format(CREATE_TABLE_STATEMENT, getCompleteName(tableName), getCompleteName(columnName), DataTypeTransformer.toPostgreSQL(dataType));
                    logger.info("[Create] execute create: {}", statement);
                    stmt.execute(statement);
                } else {
                    ResultSet columnSet = databaseMetaData.getColumns(storageUnit, "public", tableName, columnName);
                    if (!columnSet.next()) {
                        String statement = String.format(ADD_COLUMN_STATEMENT, getCompleteName(tableName), getCompleteName(columnName), DataTypeTransformer.toPostgreSQL(dataType));
                        logger.info("[Create] execute create: {}", statement);
                        stmt.execute(statement);
                    }
                    columnSet.close();
                }
                tableSet.close();
                stmt.close();
            } catch (SQLException e) {
                logger.error("create or alter table {} field {} error: {}", tableName, columnName, e.getMessage());
            }
        }
    }

    private Exception insertNonAlignedRowRecords(Connection conn, String storageUnit, RowDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(conn, storageUnit, data.getPaths(), data.getTagsList(), data.getDataTypeList());

            // 插入数据
            Map<String, Pair<String, List<String>>> tableToColumnEntries = new HashMap<>(); // <表名, <列名，值列表>>
            int cnt = 0;
            boolean firstRound = true;
            while (cnt < data.getTimeSize()) {
                int size = Math.min(data.getTimeSize() - cnt, batchSize);
                Map<String, boolean[]> tableHasData = new HashMap<>(); // 记录每一张表的每一行是否有数据点
                for (int i = cnt; i < cnt + size; i++) {
                    BitmapView bitmapView = data.getBitmapView(i);
                    int index = 0;
                    for (int j = 0; j < data.getPathNum(); j++) {
                        String path = data.getPath(j);
                        DataType dataType = data.getDataType(j);
                        String tableName = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                        String columnName = path.substring(path.lastIndexOf('.') + 1);
                        Map<String, String> tags = new HashMap<>();
                        if (data.hasTagsList()) {
                            tags = data.getTags(j);
                        }

                        StringBuilder columnKeys = new StringBuilder();
                        List<String> columnValues = new ArrayList<>();
                        if (tableToColumnEntries.containsKey(tableName)) {
                            columnKeys = new StringBuilder(tableToColumnEntries.get(tableName).k);
                            columnValues = tableToColumnEntries.get(tableName).v;
                        }

                        String value = "null";
                        if (bitmapView.get(j)) {
                            if (dataType == DataType.BINARY) {
                                value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8) + "'";
                            } else {
                                value = data.getValue(i, index).toString();
                            }
                            index++;
                            if (tableHasData.containsKey(tableName)) {
                                tableHasData.get(tableName)[i - cnt] = true;
                            } else {
                                boolean[] hasData = new boolean[size];
                                hasData[i - cnt] = true;
                                tableHasData.put(tableName, hasData);
                            }
                        }

                        if (firstRound) {
                            columnKeys.append(toFullName(columnName, tags)).append(", ");
                        }

                        if (i - cnt < columnValues.size()) {
                            columnValues.set(i - cnt, columnValues.get(i - cnt) + value + ", ");
                        } else {
                            columnValues.add(data.getKey(i) + ", " + value + ", ");  // 添加 key(time) 列
                        }

                        tableToColumnEntries.put(tableName, new Pair<>(columnKeys.toString(), columnValues));
                    }

                    firstRound = false;
                }

                for (Map.Entry<String, boolean[]> entry : tableHasData.entrySet()) {
                    String tableName = entry.getKey();
                    boolean[] hasData = entry.getValue();
                    String columnKeys = tableToColumnEntries.get(tableName).k;
                    List<String> columnValues = tableToColumnEntries.get(tableName).v;
                    boolean needToInsert = false;
                    for (int i = hasData.length - 1; i >= 0; i--) {
                        if (!hasData[i]) {
                            columnValues.remove(i);
                        } else {
                            needToInsert = true;
                        }
                    }
                    if (needToInsert) {
                        tableToColumnEntries.put(tableName, new Pair<>(columnKeys, columnValues));
                    }
                }

                executeBatchInsert(stmt, tableToColumnEntries);
                for (Pair<String, List<String>> columnEntries : tableToColumnEntries.values()) {
                    columnEntries.v.clear();
                }

                cnt += size;
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return e;
        }

        return null;
    }

    private Exception insertNonAlignedColumnRecords(Connection conn, String storageUnit, ColumnDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(conn, storageUnit, data.getPaths(), data.getTagsList(), data.getDataTypeList());

            // 插入数据
            Map<String, Pair<String, List<String>>> tableToColumnEntries = new HashMap<>(); // <表名, <列名，值列表>>
            Map<Integer, Integer> pathIndexToBitmapIndex = new HashMap<>();
            int cnt = 0;
            boolean firstRound = true;
            while (cnt < data.getTimeSize()) {
                int size = Math.min(data.getTimeSize() - cnt, batchSize);
                Map<String, boolean[]> tableHasData = new HashMap<>(); // 记录每一张表的每一行是否有数据点
                for (int i = 0; i < data.getPathNum(); i++) {
                    String path = data.getPath(i);
                    DataType dataType = data.getDataType(i);
                    String tableName = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    String columnName = path.substring(path.lastIndexOf('.') + 1);
                    Map<String, String> tags = new HashMap<>();
                    if (data.hasTagsList()) {
                        tags = data.getTags(i);
                    }

                    BitmapView bitmapView = data.getBitmapView(i);

                    StringBuilder columnKeys = new StringBuilder();
                    List<String> columnValues = new ArrayList<>();
                    if (tableToColumnEntries.containsKey(tableName)) {
                        columnKeys = new StringBuilder(tableToColumnEntries.get(tableName).k);
                        columnValues = tableToColumnEntries.get(tableName).v;
                    }

                    int index = 0;
                    if (pathIndexToBitmapIndex.containsKey(i)) {
                        index = pathIndexToBitmapIndex.get(i);
                    }
                    for (int j = cnt; j < cnt + size; j++) {
                        String value = "null";
                        if (bitmapView.get(j)) {
                            if (dataType == DataType.BINARY) {
                                value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8) + "'";
                            } else {
                                value = data.getValue(i, index).toString();
                            }
                            index++;
                            if (tableHasData.containsKey(tableName)) {
                                tableHasData.get(tableName)[j - cnt] = true;
                            } else {
                                boolean[] hasData = new boolean[size];
                                hasData[j - cnt] = true;
                                tableHasData.put(tableName, hasData);
                            }
                        }

                        if (j - cnt < columnValues.size()) {
                            columnValues.set(j - cnt, columnValues.get(j - cnt) + value + ", ");
                        } else {
                            columnValues.add(data.getKey(j) + ", " + value + ", ");  // 添加 key(time) 列
                        }
                    }
                    pathIndexToBitmapIndex.put(i, index);

                    if (firstRound) {
                        columnKeys.append(toFullName(columnName, tags)).append(", ");
                    }

                    tableToColumnEntries.put(tableName, new Pair<>(columnKeys.toString(), columnValues));
                }

                for (Map.Entry<String, boolean[]> entry : tableHasData.entrySet()) {
                    String tableName = entry.getKey();
                    boolean[] hasData = entry.getValue();
                    String columnKeys = tableToColumnEntries.get(tableName).k;
                    List<String> columnValues = tableToColumnEntries.get(tableName).v;
                    boolean needToInsert = false;
                    for (int i = hasData.length - 1; i >= 0; i--) {
                        if (!hasData[i]) {
                            columnValues.remove(i);
                        } else {
                            needToInsert = true;
                        }
                    }
                    if (needToInsert) {
                        tableToColumnEntries.put(tableName, new Pair<>(columnKeys, columnValues));
                    }
                }

                executeBatchInsert(stmt, tableToColumnEntries);
                for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
                    entry.getValue().v.clear();
                }

                firstRound = false;
                cnt += size;
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return e;
        }

        return null;
    }

    private void executeBatchInsert(Statement stmt, Map<String, Pair<String, List<String>>> tableToColumnEntries) throws SQLException {
        for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
            String tableName = entry.getKey();
            String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
            List<String> values = entry.getValue().v;
            String[] parts = columnNames.split(", ");
            boolean hasMultipleRows = parts.length != 1;

            StringBuilder statement = new StringBuilder();
            statement.append("INSERT INTO ");
            statement.append(getCompleteName(tableName));
            statement.append(" (time, ");
            statement.append(Arrays.stream(parts).map(this::getCompleteName).reduce((a, b) -> a + ", " + b).orElse(""));
            statement.append(") VALUES");
            statement.append(values.stream().map(x -> " (" + x.substring(0, x.length() - 2) + ")").reduce((a, b) -> a + ", " + b).orElse(""));
            statement.append(" ON CONFLICT (time) DO UPDATE SET ");
            if (hasMultipleRows) {
                statement.append("("); // 只有一列不加括号
            }
            statement.append(Arrays.stream(parts).map(this::getCompleteName).reduce((a, b) -> a + ", " + b).orElse(""));
            if (hasMultipleRows) {
                statement.append(")"); // 只有一列不加括号
            }
            statement.append(" = ");
            if (hasMultipleRows) {
                statement.append("("); // 只有一列不加括号
            }
            statement.append(Arrays.stream(parts).map(x -> "excluded." + getCompleteName(x)).reduce((a, b) -> a + ", " + b).orElse(""));
            if (hasMultipleRows) {
                statement.append(")"); // 只有一列不加括号
            }
            statement.append(";");

//            logger.info("[Insert] execute insert: {}", statement);
            stmt.addBatch(statement.toString());
        }
        stmt.executeBatch();
    }

    private TaskExecuteResult executeDeleteTask(Connection conn, String storageUnit, Delete delete) {
        try {
            Statement stmt = conn.createStatement();
            String statement;
            List<String> paths = delete.getPatterns();
            List<Pair<String, String>> deletedPaths; // table name -> column name
            String tableName;
            String columnName;
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet tableSet = null;
            ResultSet columnSet = null;

            if (delete.getTimeRanges() == null || delete.getTimeRanges().size() == 0) {
                if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
                    conn.close();
                    Connection postgresConn = getConnection("postgres"); // 正在使用的数据库无法被删除，因此需要切换到名为 postgres 的默认数据库
                    if (postgresConn != null) {
                        stmt = postgresConn.createStatement();
                        statement = String.format(DROP_DATABASE_STATEMENT, storageUnit);
                        logger.info("[Delete] execute delete: {}", statement);
                        stmt.execute(statement); // 删除数据库
                        stmt.close();
                        postgresConn.close();
                        return new TaskExecuteResult(null, null);
                    } else {
                        return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("cannot connect to database: postgres", new SQLException()));
                    }
                } else {
                    deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
                    for (Pair<String, String> pair : deletedPaths) {
                        tableName = pair.k;
                        columnName = pair.v;
                        tableSet = databaseMetaData.getTables(storageUnit, "public", tableName, new String[]{"TABLE"});
                        if (tableSet.next()) {
                            statement = String.format(DROP_COLUMN_STATEMENT, getCompleteName(tableName), getCompleteName(columnName));
                            logger.info("[Delete] execute delete: {}", statement);
                            stmt.execute(statement); // 删除列
                        }
                    }
                }
            } else {
                deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
                for (Pair<String, String> pair : deletedPaths) {
                    tableName = pair.k;
                    columnName = pair.v;
                    columnSet = databaseMetaData.getColumns(storageUnit, "public", tableName, columnName);
                    if (columnSet.next()) {
                        for (TimeRange timeRange : delete.getTimeRanges()) {
                            statement = String.format(UPDATE_STATEMENT, getCompleteName(tableName), getCompleteName(columnName),
                                timeRange.getBeginTime(), timeRange.getEndTime());
                            logger.info("[Delete] execute delete: {}", statement);
                            stmt.execute(statement); // 将目标列的目标范围的值置为空
                        }
                    }
                }
            }
            if (tableSet != null) {
                tableSet.close();
            }
            if (columnSet != null) {
                columnSet.close();
            }
            stmt.close();
            conn.close();
            return new TaskExecuteResult(null, null);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute delete task in postgresql failure", e));
        }
    }

    private List<Pair<String, String>> determineDeletedPaths(List<String> paths, TagFilter tagFilter) {
        List<Timeseries> timeSeries = getTimeSeries();
        List<Pair<String, String>> deletedPaths = new ArrayList<>();

        for (Timeseries ts: timeSeries) {
            for (String path : paths) {
                if (Pattern.matches(StringUtils.reformatPath(path), ts.getPath())) {
                    if (tagFilter != null && !TagKVUtils.match(ts.getTags(), tagFilter)) {
                        continue;
                    }
                    String fullPath = ts.getPath();
                    String tableName = fullPath.substring(0, fullPath.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    String columnName = toFullName(fullPath.substring(fullPath.lastIndexOf('.') + 1), ts.getTags());
                    deletedPaths.add(new Pair<>(tableName, columnName));
                    break;
                }
            }
        }

        return deletedPaths;
    }

    private String getCompleteName(String name) {
        return "\"" + name + "\"";
//        return Character.isDigit(name.charAt(0)) ? "\"" + name + "\"" : name;
    }

    @Override
    public void release() throws PhysicalException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
    }
}