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
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
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
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.postgresql.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final String IGINX_SEPARATOR = ".";

    private static final String POSTGRESQL_SEPARATOR = "$";

    private static final String DATABASE_PREFIX = "unit";

    private static final long MAX_TIMESTAMP = Integer.MAX_VALUE;

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
            //database exists
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
            logger.error("cannot get connection for database: {}", e);
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
            return executeDeleteTask(conn, delete);
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
                    if (extraParams.get("has_data") != null && extraParams.get("has_data").equals("true")) {
                    } else {
                        if (databaseName.startsWith("unit")) {
                        } else {
                            continue;
                        }
                    }
                    Connection conn1 = getConnection(databaseName, getUrl(databaseName));
                    DatabaseMetaData databaseMetaData = conn1.getMetaData();
                    ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                    while (tableSet.next()) {
                        String tableName = tableSet.getString(3);//获取表名称
                        ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
                        while (columnSet.next()) {
                            String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
                            String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                            if (databaseName.startsWith(DATABASE_PREFIX)) {
                                timeseries.add(new Timeseries(
                                    tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
                                    DataTypeTransformer.fromPostgreSQL(typeName)));
                            } else {
                                timeseries.add(new Timeseries(
                                    databaseName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR +
                                        tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
                                    DataTypeTransformer.fromPostgreSQL(typeName)));
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.info("error:", e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return timeseries;
    }


    private long toHash(String s) {
        return (long) Integer.valueOf(s);
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
                        String s = firstQuerySet.getString(0);
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


    private TaskExecuteResult executeProjectTask(Connection conn, Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            List<Field> fields = new ArrayList<>();
            for (String path : project.getPatterns()) {
                Statement stmt = conn.createStatement();
                String tableName = path.substring(0, path.lastIndexOf(".")).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String columnName = path.substring(path.lastIndexOf(".") + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = null;
                ResultSet columnSet = null;
                if (path.equals("*.*")) {
                    tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                } else if (columnName.equals("*")) {
                    columnSet = databaseMetaData.getColumns(null, null, tableName, null);
                } else {
                }

                if (tableSet == null && columnSet == null) {
                    ResultSet rs = stmt.executeQuery(String.format("select time,%s from %s where %s", columnName, tableName, FilterTransformer.toString(filter)));
                    resultSets.add(rs);
                    ResultSet columnSet_ = databaseMetaData.getColumns(null, null, tableName, columnName);
                    String typeName = "INT";
                    if (columnSet_.next()) {
                        typeName = columnSet_.getString("TYPE_NAME");//列字段类型
                    }
                    fields.add(new Field(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                        , DataTypeTransformer.fromPostgreSQL(typeName)));
                } else if (tableSet == null && columnSet != null) {
                    while (columnSet.next()) {
                        String field = columnSet.getString("COLUMN_NAME");
                        if (!field.equals("time")) {
                            String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                            ResultSet rs = stmt.executeQuery(String.format("select time,%s from %s where %s", field, tableName, FilterTransformer.toString(filter)));
                            resultSets.add(rs);
                            fields.add(new Field(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                , DataTypeTransformer.fromPostgreSQL(typeName)));
                        }
                    }
                } else {
                    while (tableSet.next()) {
                        String table = tableSet.getString(3);//获取表名称
                        ResultSet columnSet1 = databaseMetaData.getColumns(null, null, table, null);
                        while (columnSet1.next()) {
                            String field = columnSet1.getString("COLUMN_NAME");
                            if (!field.equals("time")) {
                                String typeName = columnSet1.getString("TYPE_NAME");//列字段类型
                                ResultSet rs = stmt.executeQuery(String.format("select time,%s from %s where %s", field, table, FilterTransformer.toString(filter)));
                                resultSets.add(rs);
                                fields.add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                    + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                    , DataTypeTransformer.fromPostgreSQL(typeName)));
                            }
                        }
                    }
                }
            }
            RowStream rowStream = new PostgreSQLQueryRowStream(resultSets, fields, false);
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.info("error:  ", e);
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
        }
    }


    private TaskExecuteResult executeHistoryProjectTask(Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            List<Field> fields = new ArrayList<>();
            for (String path : project.getPatterns()) {
                String database_table = path.substring(0, path.lastIndexOf("."));
                String dataBaseName = database_table.substring(0, database_table.lastIndexOf(".")).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String tableName = database_table.substring(database_table.lastIndexOf(".") + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String columnName = path.substring(path.lastIndexOf(".") + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                Connection conn = getConnection(dataBaseName, getUrl(dataBaseName));
                Statement stmt = conn.createStatement();

                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet tableSet = null;
                ResultSet columnSet = null;
                if (path.equals("*.*")) {
                    tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                } else if (columnName.equals("*")) {
                    columnSet = databaseMetaData.getColumns(null, null, tableName, null);
                }

                if (tableSet == null && columnSet == null) {
                    ResultSet columnSet2 = databaseMetaData.getColumns(null, null, tableName, null);
                    String hv = "";
                    while (columnSet2.next()) {
                        String columnName2 = columnSet2.getString("COLUMN_NAME");//获取列名称
                        hv = hv + columnName2 + ",";     //c1,c2,c3,
                    }
                    hv = hv.substring(0, hv.lastIndexOf(","));
                    ResultSet rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, columnName, tableName));
                    resultSets.add(rs);
                    ResultSet columnSet_ = databaseMetaData.getColumns(null, null, tableName, columnName);
                    String typeName = "INT";
                    if (columnSet_.next()) {
                        typeName = columnSet_.getString("TYPE_NAME");//列字段类型
                    }
                    fields.add(new Field(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                        , DataTypeTransformer.fromPostgreSQL(typeName)));
                } else if (tableSet == null && columnSet != null) {
                    while (columnSet.next()) {
                        String hv = "";
                        String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                        ResultSet columnSet_ = databaseMetaData.getColumns(null, null, tableName, null);
                        while (columnSet_.next()) {
                            String columnName2 = columnSet_.getString("COLUMN_NAME");//获取列名称
                            hv = hv + columnName2 + ",";     //c1,c2,c3,
                        }
                        hv = hv.substring(0, hv.lastIndexOf(","));

                        String field = columnSet.getString("COLUMN_NAME");
                        ResultSet rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, field, tableName));
                        resultSets.add(rs);
                        fields.add(new Field(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                            + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                            , DataTypeTransformer.fromPostgreSQL(typeName)));
                    }
                } else {
                    while (tableSet.next()) {
                        String table = tableSet.getString(3);//获取表名称
                        ResultSet columnSet1 = databaseMetaData.getColumns(null, null, table, null);
                        String hv = "";
                        while (columnSet1.next()) {
                            String columnName1 = columnSet1.getString("COLUMN_NAME");//获取列名称
                            hv = hv + columnName1 + ",";     //c1,c2,c3,
                        }
                        hv = hv.substring(0, hv.lastIndexOf(","));
                        while (columnSet1.next()) {
                            String field = columnSet1.getString("COLUMN_NAME");
                            String typeName = columnSet1.getString("TYPE_NAME");//列字段类型
                            ResultSet rs = stmt.executeQuery(String.format("select concat(%s) as time,%s from %s", hv, field, table));
                            resultSets.add(rs);
                            fields.add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                , DataTypeTransformer.fromPostgreSQL(typeName)));
                        }
                    }
                }
            }
            RowStream rowStream = new PostgreSQLQueryRowStream(resultSets, fields, true);
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            logger.info("error:  ", e);
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
                e = insertRowRecords(conn, (RowDataView) dataView);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertColumnRecords(conn, (ColumnDataView) dataView);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null,
                new PhysicalException("execute insert task in postgresql failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private void createTimeSeriesIfNotExists(Connection conn, String table, String field, Map<String, String> tags, DataType dataType) {
        try {

            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet tableSet = databaseMetaData.getTables(null, "%", table, new String[]{"TABLE"});
            if (!tableSet.next()) {
                Statement stmt = conn.createStatement();
                StringBuilder stringBuilder = new StringBuilder();
                if (tags != null && !tags.isEmpty()) {
                    for (Entry<String, String> tagsEntry : tags.entrySet()) {
                        stringBuilder.append(tagsEntry.getKey()).append(" TEXT,");
                    }
                }
                stringBuilder.append(field).append(" ").append(DataTypeTransformer.toPostgreSQL(dataType));
                stmt.execute(String
                    .format("CREATE TABLE %s (time INTEGER NOT NULL,%s NULL)", table,
                        stringBuilder.toString()));
            } else {
                if (tags != null && tags.isEmpty()) {
                    for (String tag : tags.keySet()) {
                        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, tag);
                        if (!columnSet.next()) {
                            Statement stmt = conn.createStatement();
                            stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s TEXT NULL", table, tag));
                        }
                    }
                }
                ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
                if (!columnSet.next()) {
                    Statement stmt = conn.createStatement();
                    stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s %s NULL", table, field,
                        DataTypeTransformer.toPostgreSQL(dataType)));
                }
            }
        } catch (SQLException e) {
            logger.error("create timeseries error", e);
        }
    }

    private Exception insertRowRecords(Connection conn, RowDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();
            int cnt = 0;
            for (int i = 0; i < data.getTimeSize(); i++) {
                BitmapView bitmapView = data.getBitmapView(i);
                int index = 0;
                for (int j = 0; j < data.getPathNum(); j++) {
                    if (bitmapView.get(j)) {
                        String path = data.getPath(j);
                        DataType dataType = data.getDataType(j);
                        String table = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                        String field = path.substring(path.lastIndexOf('.') + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                        Map<String, String> tags = new HashMap<>();
                        if (data.hasTagsList()) {
                            tags = data.getTags(i);
                        }
                        createTimeSeriesIfNotExists(conn, table, field, tags, dataType);

                        long time = data.getKey(i);
                        String value;
                        if (data.getDataType(j) == DataType.BINARY) {
                            value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8) + "'";
                        } else {
                            value = data.getValue(i, index).toString();
                        }

                        StringBuilder columnsKeys = new StringBuilder();
                        StringBuilder columnValues = new StringBuilder();
                        if (tags != null && !tags.isEmpty()) {
                            for (Entry<String, String> tagEntry : tags.entrySet()) {
                                columnsKeys.append(tagEntry.getValue()).append(" ");
                                columnValues.append(tagEntry.getValue()).append(" ");
                            }
                        }
                        columnsKeys.append(field);
                        columnValues.append(value);

                        stmt.addBatch(String
                            .format("INSERT INTO %s (time, %s) values (%d, %s)", table,
                                columnsKeys, time, columnValues));

                        index++;
                        cnt++;
                        if (cnt % batchSize == 0) {
                            stmt.executeBatch();
                        }

                    }
                }
            }
            stmt.executeBatch();
            conn.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return e;
        }

        return null;
    }

    private Exception insertColumnRecords(Connection conn, ColumnDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = conn.createStatement();
            int cnt = 0;
            String fields = "";
            String values = "";
            String table = "";
            long time = 0;
            for (int i = 0; i < data.getPathNum(); i++) {
                String path = data.getPath(i);
                DataType dataType = data.getDataType(i);
                table = path.substring(0, path.lastIndexOf('.')).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String field = path.substring(path.lastIndexOf('.') + 1).replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                Map<String, String> tags = new HashMap<>();
                if (data.hasTagsList()) {
                    tags = data.getTags(i);
                }
                fields = fields + "," + field;         // ,id1,id2
                createTimeSeriesIfNotExists(conn, table, field, tags, dataType);

                BitmapView bitmapView = data.getBitmapView(i);
                int index = 0;
                for (int j = 0; j < data.getTimeSize(); j++) {
                    if (bitmapView.get(j)) {
                        time = data.getKey(j);
                        String value;
                        if (data.getDataType(i) == DataType.BINARY) {
                            value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8) + "'";
                        } else {
                            value = data.getValue(i, index).toString();
                        }

                        StringBuilder columnsKeys = new StringBuilder();
                        StringBuilder columnValues = new StringBuilder();
                        if (tags != null && !tags.isEmpty()) {
                            for (Entry<String, String> tagEntry : tags.entrySet()) {
                                columnsKeys.append(tagEntry.getValue()).append(" ");
                                columnValues.append(tagEntry.getValue()).append(" ");
                            }
                        }
                        columnsKeys.append(field);
                        columnValues.append(value);
                        values = values + "," + value;   //,123,456

                        cnt++;
                        index++;
//                        stmt.addBatch(String.format("INSERT INTO %s (time, %s) values (%d, %s)", table, columnsKeys, time, columnValues));
//                        if (cnt % batchSize == 0) {
//                            stmt.executeBatch();
//                        }
                    }
//                    stmt.executeBatch();
                }
            }
            stmt.execute(String.format("INSERT INTO %s (time %s) values (%d %s)", table, fields, time, values));
        } catch (SQLException e) {
            logger.info("error", e);
            return e;
        }

        return null;
    }

    private TaskExecuteResult executeDeleteTask(Connection conn, Delete delete) {
        try {
            for (int i = 0; i < delete.getPatterns().size(); i++) {
                String path = delete.getPatterns().get(i);
                TimeRange timeRange = delete.getTimeRanges().get(i);
                String table = path.substring(0, path.lastIndexOf('.'));
                table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String field = path.substring(path.lastIndexOf('.') + 1);
                field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                // 查询序列类型
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
                if (columnSet.next()) {
                    String statement = String.format("delete from %s where (time>%d and time<%d)", table,
                        timeRange.getBeginTime(), Math.min(timeRange.getEndTime(), MAX_TIMESTAMP));
                    Statement stmt = conn.createStatement();
                    stmt.execute(statement);
                }
            }
            return new TaskExecuteResult(null, null);
        } catch (SQLException e) {
            logger.info("error:", e);
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute delete task in postgresql failure", e));
        }
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