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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
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
import cn.edu.tsinghua.iginx.postgresql.entity.PostgreSQLQueryRowStream;
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

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.POSTGRESQL_SEPARATOR;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.toFullName;

public class PostgreSQLStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

    private static final int BATCH_SIZE = 10000;

    private static final String STORAGE_ENGINE = "postgresql";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String DBNAME = "dbname";

    private static final String DEFAULT_USERNAME = "postgres";

    private static final String DEFAULT_PASSWORD = "postgres";

    private static final String DEFAULT_DBNAME = "timeseries";

    private static final String QUERY_DATABASES = "SELECT datname FROM pg_database";

    private static final String FIRST_QUERY = "select first(%s, time) from %s";

    private static final String LAST_QUERY = "select last(%s, time) from %s";

    private static final String QUERY_DATA = "SELECT time, %s FROM %s WHERE %s and %s";

    private static final String DELETE_DATA = "DELETE FROM %s WHERE time >= to_timestamp(%d) and time < to_timestamp(%d)";

    private static final String DATABASE_PREFIX = "unit";

    private static final long MAX_TIMESTAMP = Long.MAX_VALUE;

    private final StorageEngineMeta meta;

    private final Map<String, PGConnectionPoolDataSource> connectionPoolMap = new ConcurrentHashMap<>();

    private Connection connection;

    private String D_URL = "";

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
            throw new StorageInitializationException("cannot connect to " + meta.toString());
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

    private String getUrl(String dbname) {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl = String
            .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(), dbname,
                username, password);
        return connUrl;
    }

    private Connection getConnection(String dbname, String url) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format("create database %s", dbname));
        } catch (SQLException e) {
//            logger.info("database {} exists!", dbname);
        }

        try {
            if (connectionPoolMap.containsKey(dbname)) {
                return connectionPoolMap.get(dbname).getConnection();
            }
            PGConnectionPoolDataSource connectionPool = new PGConnectionPoolDataSource();
            connectionPool.setUrl(url);
            connectionPoolMap.put(dbname, connectionPool);
            return connectionPool.getConnection();
        } catch (SQLException e) {
            logger.error("cannot get connection for database {}: {}", dbname, e.getMessage());
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
        Connection conn = getConnection(storageUnit, getUrl(storageUnit));
        if (conn == null) {
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
            return isDummyStorageUnit ? executeHistoryProjectTask(project, filter) : executeProjectTask(conn, project, filter);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(conn, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(conn, storageUnit, delete);
        }
        return new TaskExecuteResult(
            new NonExecutablePhysicalTaskException("unsupported physical task in postgresql "));
    }

    @Override
    public List<Timeseries> getTimeSeries() {
        List<Timeseries> timeseries = new ArrayList<>();
        Map<String, String> extraParams = meta.getExtraParams();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES);
            while (databaseSet.next()) {
                try {
                    String databaseName = databaseSet.getString(1);//获取数据库名称
                    if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false")) && !databaseName.startsWith(DATABASE_PREFIX)) {
                        continue;
                    }

                    Connection conn = getConnection(databaseName, getUrl(databaseName));
                    if (conn == null) {
                        logger.error("cannot get connection for database {}!", databaseName);
                        return timeseries;
                    }
                    DatabaseMetaData databaseMetaData = conn.getMetaData();
                    ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                    while (tableSet.next()) {
                        String tableName = tableSet.getString(3); // 获取表名称
                        ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
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
                    }
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return timeseries;
    }


    private long toHash(String s) {
        char c[] = s.toCharArray();
        long hv = 0;
        long base = 131;
        for (int i = 0; i < c.length; i++) {
            hv = hv * base + (long) c[i];   //利用自然数溢出，即超过 LONG_MAX 自动溢出，节省时间
        }
        if (hv < 0) {
            return -1 * hv;
        }
        return hv;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        long minTime = Long.MAX_VALUE, maxTime = 0;
        List<String> paths = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES);
            while (databaseSet.next()) {
                String databaseName = databaseSet.getString(1);//获取database名称
                Connection conn2 = getConnection(databaseName, getUrl(databaseName));
                if (conn2 == null) {
                    continue;
                }
                DatabaseMetaData databaseMetaData = conn2.getMetaData();
                ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                while (tableSet.next()) {
                    String tableName = tableSet.getString(3);//获取表名称
                    ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
                    String fields = "";
                    while (columnSet.next()) {
                        String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
                        paths.add(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                            + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR));
                        fields = fields + columnName + ",";     //c1,c2,c3,
                    }
                    fields = fields.substring(0, fields.lastIndexOf(","));    //c1,c2,c3

                    // 获取first
                    String firstQueryStatement = String.format("select concat(%s) from %s", fields, tableName);
                    Statement firstQueryStmt = conn2.createStatement();
                    ResultSet firstQuerySet = firstQueryStmt.executeQuery(firstQueryStatement);
                    while (firstQuerySet.next()) {
                        String s = firstQuerySet.getString(1);
                        long logic_time = toHash(s);
                        minTime = Math.min(logic_time, minTime);
                        maxTime = Math.max(logic_time, maxTime);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        paths.sort(String::compareTo);
        return new Pair<>(new TimeSeriesInterval(paths.get(0), paths.get(paths.size() - 1)),
            new TimeInterval(minTime, maxTime + 1));
    }

    private Map<String, String> splitAndMergeQueryPatterns(Connection conn, List<String> patterns) throws SQLException {
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
            ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, columnNames);
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

    private TaskExecuteResult executeProjectTask(Connection conn, Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            ResultSet rs;
            Statement stmt;

            Map<String, String> tableNameToColumnNames = splitAndMergeQueryPatterns(conn, project.getPatterns());
            for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
                String tableName = entry.getKey();
                String columnNames = entry.getValue();
                String[] parts = columnNames.split(", ");
                StringBuilder statement = new StringBuilder();
                try {
                    stmt = conn.createStatement();
                    statement.append("select time, ");
                    for (String part : parts) {
                        statement.append(getCompleteName(part));
                        statement.append(", ");
                    }
                    statement = new StringBuilder(statement.substring(0, statement.length() - 2));
                    statement.append(" from ");
                    statement.append(getCompleteName(tableName));
                    statement.append(" where ");
                    statement.append(FilterTransformer.toString(filter));
                    rs = stmt.executeQuery(statement.toString());
                    logger.info("[Query] execute query: {}", statement);
                } catch (SQLException e) {
                    logger.error("meet error when executing query {}: {}", statement, e.getMessage());
                    continue;
                }
                resultSets.add(rs);
            }

            RowStream rowStream = new ClearEmptyRowStreamWrapper(
                new PostgreSQLQueryRowStream(resultSets, false, project.getTagFilter()));
            conn.close();
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
        }
    }

    private TaskExecuteResult executeHistoryProjectTask(Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            List<Field> fields = new ArrayList<>();
            for (String path : project.getPatterns()) {
                String[] l = path.split("\\.");
                if (l.length < 3) {
                    continue;
                }
                String database_table = path.substring(0, path.lastIndexOf("."));
                String dataBaseName = database_table.substring(0, database_table.lastIndexOf(".")).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String tableName = database_table.substring(database_table.lastIndexOf(".") + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String columnName = path.substring(path.lastIndexOf(".") + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                Connection conn = getConnection(dataBaseName, getUrl(dataBaseName));
                if (conn == null) {
                    continue;
                }
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = null;
                ResultSet columnSet = null;
                if (path.equals("*.*")) {
                    tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                } else if (columnName.equals("*")) {
                    columnSet = databaseMetaData.getColumns(null, null, tableName, null);
                }


                ResultSet columnSet_all = databaseMetaData.getColumns(null, null, tableName, null);
                String hv = "";
                while (columnSet_all.next()) {
                    String columnName2 = columnSet_all.getString("COLUMN_NAME");//获取列名称
                    hv = hv + columnName2 + ",";     //c1,c2,c3,
                }
                if (hv.equals("")) {
                    continue;
                }
                hv = hv.substring(0, hv.lastIndexOf(","));


                if (tableSet == null && columnSet == null) {
                    Statement stmt = conn.createStatement();
                    ResultSet rs = null;
                    try {
                        //String s = String.format("select concat(%s) as time,%s from %s", hv, columnName, tableName);
                        rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, columnName, tableName));
                    } catch (Exception e) {
                        continue;
                    }
                    resultSets.add(rs);
                    ResultSet columnSet_ = databaseMetaData.getColumns(null, null, tableName, columnName);
                    String typeName = "TEXT";
                    if (columnSet_.next()) {
                        typeName = columnSet_.getString("TYPE_NAME");//列字段类型
                    }
                    fields.add(new Field(dataBaseName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                        + tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                        , DataTypeTransformer.fromPostgreSQL(typeName)));
                } else if (tableSet == null && columnSet != null) {
                    ResultSet columnSet_ = databaseMetaData.getColumns(null, null, tableName, null);
                    while (columnSet_.next()) {
                        Statement stmt = conn.createStatement();
                        String typeName = columnSet_.getString("TYPE_NAME");//列字段类型
                        String field = columnSet_.getString("COLUMN_NAME");
                        ResultSet rs = null;
                        try {
                            rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, field, tableName));
                        } catch (Exception e) {
                            continue;
                        }
                        resultSets.add(rs);
                        fields.add(new Field(dataBaseName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                            + tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                            + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                            , DataTypeTransformer.fromPostgreSQL(typeName)));
                    }
                } else {
                    while (tableSet.next()) {
                        String table = tableSet.getString(3);//获取表名称
                        ResultSet columnSet2 = databaseMetaData.getColumns(null, null, table, null);
                        while (columnSet2.next()) {
                            Statement stmt = conn.createStatement();
                            String field = columnSet2.getString("COLUMN_NAME");
                            String typeName = columnSet2.getString("TYPE_NAME");//列字段类型
                            ResultSet rs = null;
                            try {
                                rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, field, table));
                            } catch (Exception e) {
                                continue;
                            }
                            resultSets.add(rs);
                            fields.add(new Field(dataBaseName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                + table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                , DataTypeTransformer.fromPostgreSQL(typeName)));
                        }
                    }
                }
            }
            RowStream rowStream = new ClearEmptyRowStreamWrapper(new PostgreSQLQueryRowStream(resultSets, true, project.getTagFilter()));
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.info(e.getMessage());
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
        }
    }


    private TaskExecuteResult executeInsertTask(Connection conn, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertNonAlignedRowRecords(conn, (RowDataView) dataView);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords(conn, (ColumnDataView) dataView);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null,
                new PhysicalException("execute insert task in postgresql failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private void createOrAlterTables(Connection conn, List<String> paths, List<Map<String, String>> tagsList, List<DataType> dataTypeList) {
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            Map<String, String> tags = new HashMap<>();
            if (tagsList != null && !tagsList.isEmpty()) {
                tags = tagsList.get(i);
            }
            DataType dataType = dataTypeList.get(i);
            String table = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            String field = path.substring(path.lastIndexOf('.') + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);

            try {
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = databaseMetaData.getTables(null, "%", table, new String[]{"TABLE"});
                if (!tableSet.next()) {
                    Statement stmt = conn.createStatement();
                    String columnName = toFullName(field, tags);
                    String statement = String.format("CREATE TABLE %s (time BIGINT NOT NULL, %s %s, PRIMARY KEY(time))",
                        getCompleteName(table), getCompleteName(columnName), DataTypeTransformer.toPostgreSQL(dataType));
                    logger.info("[Create] execute create: {}", statement);
                    stmt.execute(statement);
                } else {
                    String columnName = toFullName(field, tags);
                    ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, columnName);
                    if (!columnSet.next()) {
                        Statement stmt = conn.createStatement();
                        String statement = String.format("ALTER TABLE %s ADD COLUMN %s %s NULL",
                            getCompleteName(table), getCompleteName(columnName), DataTypeTransformer.toPostgreSQL(dataType));
                        logger.info("[Create] execute create: {}", statement);
                        stmt.execute(statement);
                    }
                }
            } catch (SQLException e) {
                logger.error("create or alter table {} field {} error: {}", table, field, e.getMessage());
            }
        }
    }

    private Exception insertNonAlignedRowRecords(Connection conn, RowDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(conn, data.getPaths(), data.getTagsList(), data.getDataTypeList());

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
                        String table = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                        String field = path.substring(path.lastIndexOf('.') + 1);
                        Map<String, String> tags = new HashMap<>();
                        if (data.hasTagsList()) {
                            tags = data.getTags(j);
                        }

                        StringBuilder columnKeys = new StringBuilder();
                        List<String> columnValues = new ArrayList<>();
                        if (tableToColumnEntries.containsKey(table)) {
                            columnKeys = new StringBuilder(tableToColumnEntries.get(table).k);
                            columnValues = tableToColumnEntries.get(table).v;
                        }

                        String value = "null";
                        if (bitmapView.get(j)) {
                            if (dataType == DataType.BINARY) {
                                value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8) + "'";
                            } else {
                                value = data.getValue(i, index).toString();
                            }
                            index++;
                            if (tableHasData.containsKey(table)) {
                                tableHasData.get(table)[i - cnt] = true;
                            } else {
                                boolean[] hasData = new boolean[size];
                                hasData[i - cnt] = true;
                                tableHasData.put(table, hasData);
                            }
                        }

                        if (firstRound) {
                            columnKeys.append(toFullName(field, tags)).append(", ");
                        }

                        if (i - cnt < columnValues.size()) {
                            columnValues.set(i - cnt, columnValues.get(i - cnt) + value + ", ");
                        } else {
                            columnValues.add(data.getKey(i) + ", " + value + ", ");  // 添加 key(time) 列
                        }

                        tableToColumnEntries.put(table, new Pair<>(columnKeys.toString(), columnValues));
                    }

                    firstRound = false;
                }

                for (Map.Entry<String, boolean[]> entry : tableHasData.entrySet()) {
                    String table = entry.getKey();
                    boolean[] hasData = entry.getValue();
                    String columnKeys = tableToColumnEntries.get(table).k;
                    List<String> columnValues = tableToColumnEntries.get(table).v;
                    boolean needToInsert = false;
                    for (int i = hasData.length - 1; i >= 0; i--) {
                        if (!hasData[i]) {
                            columnValues.remove(i);
                        } else {
                            needToInsert = true;
                        }
                    }
                    if (needToInsert) {
                        tableToColumnEntries.put(table, new Pair<>(columnKeys, columnValues));
                    }
                }

                executeBatchInsert(stmt, tableToColumnEntries);
                for (Pair<String, List<String>> columnEntries : tableToColumnEntries.values()) {
                    columnEntries.v.clear();
                }

                cnt += size;
            }
            conn.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return e;
        }

        return null;
    }

    private Exception insertNonAlignedColumnRecords(Connection conn, ColumnDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();

            // 创建表
            createOrAlterTables(conn, data.getPaths(), data.getTagsList(), data.getDataTypeList());

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
                    String table = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                    String field = path.substring(path.lastIndexOf('.') + 1);
                    Map<String, String> tags = new HashMap<>();
                    if (data.hasTagsList()) {
                        tags = data.getTags(i);
                    }

                    BitmapView bitmapView = data.getBitmapView(i);

                    StringBuilder columnKeys = new StringBuilder();
                    List<String> columnValues = new ArrayList<>();
                    if (tableToColumnEntries.containsKey(table)) {
                        columnKeys = new StringBuilder(tableToColumnEntries.get(table).k);
                        columnValues = tableToColumnEntries.get(table).v;
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
                            if (tableHasData.containsKey(table)) {
                                tableHasData.get(table)[j - cnt] = true;
                            } else {
                                boolean[] hasData = new boolean[size];
                                hasData[j - cnt] = true;
                                tableHasData.put(table, hasData);
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
                        columnKeys.append(toFullName(field, tags)).append(", ");
                    }

                    tableToColumnEntries.put(table, new Pair<>(columnKeys.toString(), columnValues));
                }

                for (Map.Entry<String, boolean[]> entry : tableHasData.entrySet()) {
                    String table = entry.getKey();
                    boolean[] hasData = entry.getValue();
                    String columnKeys = tableToColumnEntries.get(table).k;
                    List<String> columnValues = tableToColumnEntries.get(table).v;
                    boolean needToInsert = false;
                    for (int i = hasData.length - 1; i >= 0; i--) {
                        if (!hasData[i]) {
                            columnValues.remove(i);
                        } else {
                            needToInsert = true;
                        }
                    }
                    if (needToInsert) {
                        tableToColumnEntries.put(table, new Pair<>(columnKeys, columnValues));
                    }
                }

                executeBatchInsert(stmt, tableToColumnEntries);
                for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
                    entry.getValue().v.clear();
                }

                firstRound = false;
                cnt += size;
            }
            conn.close();
        } catch (SQLException e) {
            logger.info("error", e);
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

            StringBuilder insertStatement = new StringBuilder();
            insertStatement.append("INSERT INTO ");
            insertStatement.append(getCompleteName(tableName));
            insertStatement.append(" (time, ");
            for (String part : parts) {
                insertStatement.append(getCompleteName(part));
                insertStatement.append(", ");
            }
            insertStatement = new StringBuilder(insertStatement.substring(0, insertStatement.length() - 2));
            insertStatement.append(") VALUES");
            for (String value : values) {
                insertStatement.append(" (");
                insertStatement.append(value, 0, value.length() - 2);
                insertStatement.append("), ");
            }
            insertStatement = new StringBuilder(insertStatement.substring(0, insertStatement.length() - 2));

            insertStatement.append(" ON CONFLICT (time) DO UPDATE SET ");
            if (hasMultipleRows) {
                insertStatement.append("("); // 只有一列不加括号
            }
            for (String part : parts) {
                insertStatement.append(getCompleteName(part));
                insertStatement.append(", ");
            }
            insertStatement = new StringBuilder(insertStatement.substring(0, insertStatement.length() - 2));
            if (hasMultipleRows) {
                insertStatement.append(")"); // 只有一列不加括号
            }
            insertStatement.append(" = ");
            if (hasMultipleRows) {
                insertStatement.append("("); // 只有一列不加括号
            }
            for (String part : parts) {
                insertStatement.append("excluded.");
                insertStatement.append(getCompleteName(part));
                insertStatement.append(", ");
            }
            insertStatement = new StringBuilder(insertStatement.substring(0, insertStatement.length() - 2));
            if (hasMultipleRows) {
                insertStatement.append(")"); // 只有一列不加括号
            }

//            logger.info("[Insert] execute insert: {}", insertStatement);
            stmt.addBatch(insertStatement.toString());
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
                    Connection postgresConn = getConnection("postgres", getUrl("postgres")); // 正在使用的数据库无法被删除，因此需要切换到名为postgres的默认数据库
                    if (postgresConn != null) {
                        stmt = postgresConn.createStatement();
                        statement = String.format("drop database %s", storageUnit);
                        logger.info("[Delete] execute delete: {}", statement);
                        stmt.execute(statement); // 删除数据库
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
                        tableSet = databaseMetaData.getTables(null, "%", tableName, new String[]{"TABLE"});
                        if (tableSet.next()) {
                            statement = String.format("alter table %s drop column if exists %s", getCompleteName(tableName), getCompleteName(columnName));
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
                    columnSet = databaseMetaData.getColumns(null, "%", tableName, columnName);
                    if (columnSet.next()) {
                        for (TimeRange timeRange : delete.getTimeRanges()) {
                            statement = String.format("update %s set %s = null where (time >= %d and time < %d)", getCompleteName(tableName), getCompleteName(columnName),
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