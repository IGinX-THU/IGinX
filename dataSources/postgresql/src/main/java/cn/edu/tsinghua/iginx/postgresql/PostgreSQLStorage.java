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

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.*;
import static cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer.fromPostgreSQL;
import static cn.edu.tsinghua.iginx.postgresql.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
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
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.postgresql.query.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

    private final StorageEngineMeta meta;

    private final Map<String, PGConnectionPoolDataSource> connectionPoolMap =
            new ConcurrentHashMap<>();

    private final Connection connection;

    public PostgreSQLStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl =
                String.format(
                        "jdbc:postgresql://%s:%s/?user=%s&password=%s",
                        meta.getIp(), meta.getPort(), username, password);
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
        String connUrl =
                String.format(
                        "jdbc:postgresql://%s:%s/?user=%s&password=%s",
                        meta.getIp(), meta.getPort(), username, password);
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
        String connUrl =
                String.format(
                        "jdbc:postgresql://%s:%s/%s?user=%s&password=%s",
                        meta.getIp(), meta.getPort(), databaseName, username, password);
        return connUrl;
    }

    private Connection getConnection(String databaseName) {
        if (databaseName.startsWith("dummy")) {
            return null;
        }
        if (databaseName.equalsIgnoreCase("template0")
                || databaseName.equalsIgnoreCase("template1")) {
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
    public List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        Map<String, String> extraParams = meta.getExtraParams();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
            while (databaseSet.next()) {
                try {
                    String databaseName = databaseSet.getString("DATNAME"); // 获取数据库名称
                    if ((extraParams.get("has_data") == null
                                    || extraParams.get("has_data").equals("false"))
                            && !databaseName.startsWith(DATABASE_PREFIX)) {
                        continue;
                    }
                    Connection conn = getConnection(databaseName);
                    if (conn == null) {
                        continue;
                    }
                    DatabaseMetaData databaseMetaData = conn.getMetaData();
                    ResultSet tableSet =
                            databaseMetaData.getTables(
                                    databaseName, "public", "%", new String[] {"TABLE"});
                    while (tableSet.next()) {
                        String tableName = tableSet.getString("TABLE_NAME"); // 获取表名称
                        ResultSet columnSet =
                                databaseMetaData.getColumns(databaseName, "public", tableName, "%");
                        while (columnSet.next()) {
                            String columnName = columnSet.getString("COLUMN_NAME"); // 获取列名称
                            String typeName = columnSet.getString("TYPE_NAME"); // 列字段类型
                            if (columnName.equals(KEY_NAME)) { // key 列不显示
                                continue;
                            }
                            Pair<String, Map<String, String>> nameAndTags =
                                    splitFullName(columnName);
                            if (databaseName.startsWith(DATABASE_PREFIX)) {
                                columns.add(
                                        new Column(
                                                tableName.replace(
                                                                POSTGRESQL_SEPARATOR,
                                                                IGINX_SEPARATOR)
                                                        + IGINX_SEPARATOR
                                                        + nameAndTags.k,
                                                fromPostgreSQL(typeName),
                                                nameAndTags.v));
                            } else {
                                columns.add(
                                        new Column(
                                                databaseName.replace(
                                                                POSTGRESQL_SEPARATOR,
                                                                IGINX_SEPARATOR)
                                                        + IGINX_SEPARATOR
                                                        + tableName.replace(
                                                                POSTGRESQL_SEPARATOR,
                                                                IGINX_SEPARATOR)
                                                        + IGINX_SEPARATOR
                                                        + nameAndTags.k,
                                                fromPostgreSQL(typeName),
                                                nameAndTags.v));
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
        return columns;
    }

    @Override
    public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
        try {
            String databaseName = dataArea.getStorageUnit();
            Connection conn = getConnection(databaseName);
            if (conn == null) {
                return new TaskExecuteResult(
                        new PhysicalTaskExecuteFailureException(
                                String.format("cannot connect to database %s", databaseName)));
            }
            KeyInterval keyInterval = dataArea.getKeyInterval();
            Filter filter =
                    new AndFilter(
                            Arrays.asList(
                                    new KeyFilter(Op.GE, keyInterval.getStartKey()),
                                    new KeyFilter(Op.L, keyInterval.getEndKey())));

            List<String> databaseNameList = new ArrayList<>();
            List<ResultSet> resultSets = new ArrayList<>();
            ResultSet rs;
            Statement stmt;

            Map<String, String> tableNameToColumnNames =
                    splitAndMergeQueryPatterns(databaseName, conn, project.getPatterns());
            for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
                String tableName = entry.getKey();
                String fullColumnNames = getFullColumnNames(entry.getValue());
                String statement =
                        String.format(
                                QUERY_STATEMENT,
                                fullColumnNames,
                                getFullName(tableName),
                                FilterTransformer.toString(filter));
                try {
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(statement);
                    logger.info("[Query] execute query: {}", statement);
                } catch (SQLException e) {
                    logger.error(
                            "meet error when executing query {}: {}", statement, e.getMessage());
                    continue;
                }
                databaseNameList.add(databaseName);
                resultSets.add(rs);
            }

            RowStream rowStream =
                    new ClearEmptyRowStreamWrapper(
                            new PostgreSQLQueryRowStream(
                                    databaseNameList,
                                    resultSets,
                                    false,
                                    filter,
                                    project.getTagFilter()));
            conn.close();
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                    new PhysicalTaskExecuteFailureException(
                            "execute project task in postgresql failure", e));
        }
    }

    @Override
    public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
        try {
            KeyInterval keyInterval = dataArea.getKeyInterval();
            Filter filter =
                    new AndFilter(
                            Arrays.asList(
                                    new KeyFilter(Op.GE, keyInterval.getStartKey()),
                                    new KeyFilter(Op.L, keyInterval.getEndKey())));

            List<String> databaseNameList = new ArrayList<>();
            List<ResultSet> resultSets = new ArrayList<>();
            ResultSet rs;
            Connection conn = null;
            Statement stmt;

            Map<String, Map<String, String>> splitResults =
                    splitAndMergeHistoryQueryPatterns(project.getPatterns());
            for (Map.Entry<String, Map<String, String>> splitEntry : splitResults.entrySet()) {
                String databaseName = splitEntry.getKey();
                conn = getConnection(databaseName);
                if (conn == null) {
                    continue;
                }
                for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
                    String tableName = entry.getKey();
                    String fullColumnNames = getFullColumnNames(entry.getValue());
                    String statement =
                            String.format(
                                    CONCAT_QUERY_STATEMENT_WITHOUT_WHERE_CLAUSE,
                                    fullColumnNames,
                                    fullColumnNames,
                                    getFullName(tableName));

                    try {
                        stmt = conn.createStatement();
                        rs = stmt.executeQuery(statement);
                        logger.info("[Query] execute query: {}", statement);
                    } catch (SQLException e) {
                        logger.error(
                                "meet error when executing query {}: {}",
                                statement,
                                e.getMessage());
                        continue;
                    }
                    databaseNameList.add(databaseName);
                    resultSets.add(rs);
                }
            }

            RowStream rowStream =
                    new ClearEmptyRowStreamWrapper(
                            new PostgreSQLQueryRowStream(
                                    databaseNameList,
                                    resultSets,
                                    true,
                                    filter,
                                    project.getTagFilter()));
            if (conn != null) {
                conn.close();
            }
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                    new PhysicalTaskExecuteFailureException(
                            "execute project task in postgresql failure", e));
        }
    }

    @Override
    public boolean isSupportProjectWithSelect() {
        return false;
    }

    @Override
    public TaskExecuteResult executeProjectWithSelect(
            Project project, Select select, DataArea dataArea) {
        return null;
    }

    @Override
    public TaskExecuteResult executeProjectDummyWithSelect(
            Project project, Select select, DataArea dataArea) {
        return null;
    }

    @Override
    public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
        try {
            String databaseName = dataArea.getStorageUnit();
            Connection conn = getConnection(databaseName);
            if (conn == null) {
                return new TaskExecuteResult(
                        new PhysicalTaskExecuteFailureException(
                                String.format("cannot connect to database %s", databaseName)));
            }

            Statement stmt = conn.createStatement();
            String statement;
            List<String> paths = delete.getPatterns();
            List<Pair<String, String>> deletedPaths; // table name -> column name
            String tableName;
            String columnName;
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet tableSet = null;
            ResultSet columnSet = null;

            if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
                if (paths.size() == 1
                        && paths.get(0).equals("*")
                        && delete.getTagFilter() == null) {
                    conn.close();
                    Connection postgresConn =
                            getConnection("postgres"); // 正在使用的数据库无法被删除，因此需要切换到名为 postgres 的默认数据库
                    if (postgresConn != null) {
                        stmt = postgresConn.createStatement();
                        statement = String.format(DROP_DATABASE_STATEMENT, databaseName);
                        logger.info("[Delete] execute delete: {}", statement);
                        stmt.execute(statement); // 删除数据库
                        stmt.close();
                        postgresConn.close();
                        return new TaskExecuteResult(null, null);
                    } else {
                        return new TaskExecuteResult(
                                new PhysicalTaskExecuteFailureException(
                                        "cannot connect to database postgres", new SQLException()));
                    }
                } else {
                    deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
                    for (Pair<String, String> pair : deletedPaths) {
                        tableName = pair.k;
                        columnName = pair.v;
                        tableSet =
                                databaseMetaData.getTables(
                                        databaseName, "public", tableName, new String[] {"TABLE"});
                        if (tableSet.next()) {
                            statement =
                                    String.format(
                                            DROP_COLUMN_STATEMENT,
                                            getFullName(tableName),
                                            getFullName(columnName));
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
                    columnSet =
                            databaseMetaData.getColumns(
                                    databaseName, "public", tableName, columnName);
                    if (columnSet.next()) {
                        for (KeyRange keyRange : delete.getKeyRanges()) {
                            statement =
                                    String.format(
                                            UPDATE_STATEMENT,
                                            getFullName(tableName),
                                            getFullName(columnName),
                                            keyRange.getBeginKey(),
                                            keyRange.getEndKey());
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
                    new PhysicalTaskExecuteFailureException(
                            "execute delete task in postgresql failure", e));
        }
    }

    @Override
    public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
        DataView dataView = insert.getData();
        String databaseName = dataArea.getStorageUnit();
        Connection conn = getConnection(databaseName);
        if (conn == null) {
            return new TaskExecuteResult(
                    new PhysicalTaskExecuteFailureException(
                            String.format("cannot connect to database %s", databaseName)));
        }
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertNonAlignedRowRecords(conn, databaseName, (RowDataView) dataView);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords(conn, databaseName, (ColumnDataView) dataView);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(
                    null, new PhysicalException("execute insert task in postgresql failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    @Override
    public Pair<ColumnsRange, KeyInterval> getBoundaryOfStorage(String dataPrefix)
            throws PhysicalException {
        ColumnsRange columnsRange;
        long minKey = Long.MAX_VALUE;
        long maxKey = 0;
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
                ResultSet tableSet =
                        databaseMetaData.getTables(
                                databaseName, "public", "%", new String[] {"TABLE"});
                while (tableSet.next()) {
                    String tableName = tableSet.getString("TABLE_NAME"); // 获取表名称
                    if (dataPrefix != null
                            && !tableName.startsWith(
                                    dataPrefix.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR))) {
                        continue;
                    }
                    ResultSet columnSet =
                            databaseMetaData.getColumns(databaseName, "public", tableName, "%");
                    StringBuilder columnNames = new StringBuilder();
                    while (columnSet.next()) {
                        String columnName = columnSet.getString("COLUMN_NAME"); // 获取列名称
                        paths.add(
                                databaseName
                                        + IGINX_SEPARATOR
                                        + tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                        + IGINX_SEPARATOR
                                        + columnName);
                        columnNames.append(columnName);
                        columnNames.append(", "); // c1, c2, c3,
                    }
                    columnNames =
                            new StringBuilder(
                                    columnNames.substring(
                                            0, columnNames.length() - 2)); // c1, c2, c3

                    // 获取 key 的范围
                    String statement =
                            String.format(
                                    CONCAT_QUERY_STATEMENT,
                                    getFullColumnNames(columnNames.toString()),
                                    getFullName(tableName));
                    Statement concatStmt = conn.createStatement();
                    ResultSet concatSet = concatStmt.executeQuery(statement);
                    while (concatSet.next()) {
                        String concatValue = concatSet.getString("concat");
                        long key = toHash(concatValue);
                        minKey = Math.min(key, minKey);
                        maxKey = Math.max(key, maxKey);
                    }
                    concatSet.close();
                    concatStmt.close();
                }
                tableSet.close();
                conn.close();
            }
            databaseSet.close();
            stmt.close();
        } catch (SQLException e) {
            logger.error("encounter error when getting boundary of storage: {}", e.getMessage());
        }
        paths.sort(String::compareTo);

        if (dataPrefix != null) {
            columnsRange = new ColumnsInterval(dataPrefix, StringUtils.nextString(dataPrefix));
        } else {
            columnsRange = new ColumnsInterval(paths.get(0), paths.get(paths.size() - 1));
        }

        return new Pair<>(columnsRange, new KeyInterval(minKey, maxKey + 1));
    }

    private Map<String, String> splitAndMergeQueryPatterns(
            String databaseName, Connection conn, List<String> patterns) throws SQLException {
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
                    tableName =
                            pattern.substring(0, pattern.lastIndexOf(IGINX_SEPARATOR))
                                    .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    columnNames = pattern.substring(pattern.lastIndexOf(IGINX_SEPARATOR) + 1);
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
            ResultSet rs =
                    conn.getMetaData().getColumns(databaseName, "public", tableName, columnNames);
            while (rs.next()) {
                tableName = rs.getString("TABLE_NAME");
                columnNames = rs.getString("COLUMN_NAME");
                if (columnNames.equals(KEY_NAME)) {
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

    private Map<String, Map<String, String>> splitAndMergeHistoryQueryPatterns(
            List<String> patterns) throws SQLException {
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

                databaseName = parts[0];
                if (parts.length == 1) { // 只有一级
                    tableName = "%";
                    columnNames = "%";
                } else if (parts.length == 2) {
                    tableName = parts[1].equals("*") ? "%" : parts[1];
                    columnNames = "%";
                } else {
                    tableName =
                            pattern.substring(
                                            pattern.indexOf(IGINX_SEPARATOR) + 1,
                                            pattern.lastIndexOf(IGINX_SEPARATOR))
                                    .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR)
                                    .replace("*", "%");
                    columnNames = pattern.substring(pattern.lastIndexOf(IGINX_SEPARATOR) + 1);
                    if (columnNames.equals("*")) {
                        tableName += "%";
                        columnNames = "%";
                    }
                }
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
                    ResultSet tableSet =
                            databaseMetaData.getTables(
                                    tempDatabaseName, "public", tableName, new String[] {"TABLE"});
                    while (tableSet.next()) {
                        String tempTableName = tableSet.getString("TABLE_NAME");
                        ResultSet columnSet =
                                databaseMetaData.getColumns(
                                        tempDatabaseName, "public", tempTableName, columnNames);
                        while (columnSet.next()) {
                            String tempColumnNames = columnSet.getString("COLUMN_NAME");
                            Map<String, String> tableNameToColumnNames = new HashMap<>();
                            if (splitResults.containsKey(tempDatabaseName)) {
                                tableNameToColumnNames = splitResults.get(tempDatabaseName);
                                tempColumnNames =
                                        tableNameToColumnNames.get(tempTableName)
                                                + ", "
                                                + tempColumnNames;
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
                ResultSet rs =
                        conn.getMetaData()
                                .getColumns(databaseName, "public", tableName, columnNames);
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

    private void createOrAlterTables(
            Connection conn,
            String storageUnit,
            List<String> paths,
            List<Map<String, String>> tagsList,
            List<DataType> dataTypeList) {
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            Map<String, String> tags = new HashMap<>();
            if (tagsList != null && !tagsList.isEmpty()) {
                tags = tagsList.get(i);
            }
            DataType dataType = dataTypeList.get(i);
            String tableName =
                    path.substring(0, path.lastIndexOf(IGINX_SEPARATOR))
                            .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            String columnName =
                    path.substring(path.lastIndexOf(IGINX_SEPARATOR) + 1)
                            .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);

            try {
                Statement stmt = conn.createStatement();
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet =
                        databaseMetaData.getTables(
                                storageUnit, "public", tableName, new String[] {"TABLE"});
                columnName = toFullName(columnName, tags);
                if (!tableSet.next()) {
                    String statement =
                            String.format(
                                    CREATE_TABLE_STATEMENT,
                                    getFullName(tableName),
                                    getFullName(columnName),
                                    DataTypeTransformer.toPostgreSQL(dataType));
                    logger.info("[Create] execute create: {}", statement);
                    stmt.execute(statement);
                } else {
                    ResultSet columnSet =
                            databaseMetaData.getColumns(
                                    storageUnit, "public", tableName, columnName);
                    if (!columnSet.next()) {
                        String statement =
                                String.format(
                                        ADD_COLUMN_STATEMENT,
                                        getFullName(tableName),
                                        getFullName(columnName),
                                        DataTypeTransformer.toPostgreSQL(dataType));
                        logger.info("[Create] execute create: {}", statement);
                        stmt.execute(statement);
                    }
                    columnSet.close();
                }
                tableSet.close();
                stmt.close();
            } catch (SQLException e) {
                logger.error(
                        "create or alter table {} field {} error: {}",
                        tableName,
                        columnName,
                        e.getMessage());
            }
        }
    }

    private Exception insertNonAlignedRowRecords(
            Connection conn, String databaseName, RowDataView data) {
        int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(
                    conn,
                    databaseName,
                    data.getPaths(),
                    data.getTagsList(),
                    data.getDataTypeList());

            // 插入数据
            Map<String, Pair<String, List<String>>> tableToColumnEntries =
                    new HashMap<>(); // <表名, <列名，值列表>>
            int cnt = 0;
            boolean firstRound = true;
            while (cnt < data.getKeySize()) {
                int size = Math.min(data.getKeySize() - cnt, batchSize);
                Map<String, boolean[]> tableHasData = new HashMap<>(); // 记录每一张表的每一行是否有数据点
                for (int i = cnt; i < cnt + size; i++) {
                    BitmapView bitmapView = data.getBitmapView(i);
                    int index = 0;
                    for (int j = 0; j < data.getPathNum(); j++) {
                        String path = data.getPath(j);
                        DataType dataType = data.getDataType(j);
                        String tableName =
                                path.substring(0, path.lastIndexOf(IGINX_SEPARATOR))
                                        .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                        String columnName = path.substring(path.lastIndexOf(IGINX_SEPARATOR) + 1);
                        Map<String, String> tags = data.getTags(j);

                        StringBuilder columnKeys = new StringBuilder();
                        List<String> columnValues = new ArrayList<>();
                        if (tableToColumnEntries.containsKey(tableName)) {
                            columnKeys = new StringBuilder(tableToColumnEntries.get(tableName).k);
                            columnValues = tableToColumnEntries.get(tableName).v;
                        }

                        String value = "null";
                        if (bitmapView.get(j)) {
                            if (dataType == DataType.BINARY) {
                                value =
                                        "'"
                                                + new String(
                                                        (byte[]) data.getValue(i, index),
                                                        StandardCharsets.UTF_8)
                                                + "'";
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
                            columnValues.add(data.getKey(i) + ", " + value + ", "); // 添加 key 列
                        }

                        tableToColumnEntries.put(
                                tableName, new Pair<>(columnKeys.toString(), columnValues));
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

    private Exception insertNonAlignedColumnRecords(
            Connection conn, String databaseName, ColumnDataView data) {
        int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(
                    conn,
                    databaseName,
                    data.getPaths(),
                    data.getTagsList(),
                    data.getDataTypeList());

            // 插入数据
            Map<String, Pair<String, List<String>>> tableToColumnEntries =
                    new HashMap<>(); // <表名, <列名，值列表>>
            Map<Integer, Integer> pathIndexToBitmapIndex = new HashMap<>();
            int cnt = 0;
            boolean firstRound = true;
            while (cnt < data.getKeySize()) {
                int size = Math.min(data.getKeySize() - cnt, batchSize);
                Map<String, boolean[]> tableHasData = new HashMap<>(); // 记录每一张表的每一行是否有数据点
                for (int i = 0; i < data.getPathNum(); i++) {
                    String path = data.getPath(i);
                    DataType dataType = data.getDataType(i);
                    String tableName =
                            path.substring(0, path.lastIndexOf(IGINX_SEPARATOR))
                                    .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    String columnName = path.substring(path.lastIndexOf(IGINX_SEPARATOR) + 1);
                    Map<String, String> tags = data.getTags(i);
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
                                value =
                                        "'"
                                                + new String(
                                                        (byte[]) data.getValue(i, index),
                                                        StandardCharsets.UTF_8)
                                                + "'";
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
                            columnValues.add(data.getKey(j) + ", " + value + ", "); // 添加 key 列
                        }
                    }
                    pathIndexToBitmapIndex.put(i, index);

                    if (firstRound) {
                        columnKeys.append(toFullName(columnName, tags)).append(", ");
                    }

                    tableToColumnEntries.put(
                            tableName, new Pair<>(columnKeys.toString(), columnValues));
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
                for (Map.Entry<String, Pair<String, List<String>>> entry :
                        tableToColumnEntries.entrySet()) {
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

    private void executeBatchInsert(
            Statement stmt, Map<String, Pair<String, List<String>>> tableToColumnEntries)
            throws SQLException {
        for (Map.Entry<String, Pair<String, List<String>>> entry :
                tableToColumnEntries.entrySet()) {
            String tableName = entry.getKey();
            String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
            List<String> values = entry.getValue().v;
            String[] parts = columnNames.split(", ");
            boolean hasMultipleRows = parts.length != 1;

            // INSERT INTO XXX ("\u2E85", XXX, ...) VALUES (XXX, XXX, ...), (XXX, XXX, ...), ...,
            // (XXX,
            // XXX, ...) ON CONFLICT ("\u2E85") DO UPDATE SET (XXX, ...) = (excluded.XXX, ...);
            StringBuilder statement = new StringBuilder();
            statement.append("INSERT INTO ");
            statement.append(getFullName(tableName));
            statement.append(" (");
            statement.append(KEY_NAME);
            statement.append(", ");
            String fullColumnNames = getFullColumnNames(columnNames);
            statement.append(fullColumnNames);

            statement.append(") VALUES ");
            for (String value : values) {
                statement.append("(");
                statement.append(value, 0, value.length() - 2);
                statement.append("), ");
            }
            statement = new StringBuilder(statement.substring(0, statement.length() - 2));

            statement.append(" ON CONFLICT (");
            statement.append(KEY_NAME);
            statement.append(") DO UPDATE SET ");
            if (hasMultipleRows) {
                statement.append("("); // 只有一列不加括号
            }
            statement.append(fullColumnNames);
            if (hasMultipleRows) {
                statement.append(")"); // 只有一列不加括号
            }
            statement.append(" = ");
            if (hasMultipleRows) {
                statement.append("("); // 只有一列不加括号
            }
            for (String part : parts) {
                statement.append("excluded.");
                statement.append(getFullName(part));
                statement.append(", ");
            }
            statement = new StringBuilder(statement.substring(0, statement.length() - 2));
            if (hasMultipleRows) {
                statement.append(")"); // 只有一列不加括号
            }
            statement.append(";");

            //            logger.info("[Insert] execute insert: {}", statement);
            stmt.addBatch(statement.toString());
        }
        stmt.executeBatch();
    }

    private List<Pair<String, String>> determineDeletedPaths(
            List<String> paths, TagFilter tagFilter) {
        List<Column> columns = getColumns();
        List<Pair<String, String>> deletedPaths = new ArrayList<>();

        for (Column column : columns) {
            for (String path : paths) {
                if (Pattern.matches(StringUtils.reformatPath(path), column.getPath())) {
                    if (tagFilter != null && !TagKVUtils.match(column.getTags(), tagFilter)) {
                        continue;
                    }
                    String fullPath = column.getPath();
                    String tableName =
                            fullPath.substring(0, fullPath.lastIndexOf(IGINX_SEPARATOR))
                                    .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    String columnName =
                            toFullName(
                                    fullPath.substring(fullPath.lastIndexOf(IGINX_SEPARATOR) + 1),
                                    column.getTags());
                    deletedPaths.add(new Pair<>(tableName, columnName));
                    break;
                }
            }
        }

        return deletedPaths;
    }

    private String getFullName(String name) {
        return "\"" + name + "\"";
        //        return Character.isDigit(name.charAt(0)) ? "\"" + name + "\"" : name;
    }

    private String getFullColumnNames(String columnNames) {
        String[] parts = columnNames.split(", ");
        StringBuilder fullColumnNames = new StringBuilder();
        for (String part : parts) {
            fullColumnNames.append(getFullName(part));
            fullColumnNames.append(", ");
        }
        return fullColumnNames.substring(0, fullColumnNames.length() - 2);
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
