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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.*;
import static cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer.fromPostgreSQL;
import static cn.edu.tsinghua.iginx.postgresql.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
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
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.postgresql.query.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.PostgreSQLSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
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
    if (!meta.getStorageEngine().equals(StorageEngineType.postgresql)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
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

  private boolean filterContainsType(List<FilterType> types, Filter filter) {
    boolean res = false;
    if (types.contains(filter.getType())) {
      return true;
    }
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        res |= filterContainsType(types, notChild);
        break;
      default:
        break;
    }
    return res;
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
          if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false"))
              && !databaseName.startsWith(DATABASE_PREFIX)) {
            continue;
          }
          Connection conn = getConnection(databaseName);
          if (conn == null) {
            continue;
          }
          DatabaseMetaData databaseMetaData = conn.getMetaData();
          ResultSet tableSet =
              databaseMetaData.getTables(databaseName, "public", "%", new String[] {"TABLE"});
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
              Pair<String, Map<String, String>> nameAndTags = splitFullName(columnName);
              if (databaseName.startsWith(DATABASE_PREFIX)) {
                columns.add(
                    new Column(
                        tableName + SEPARATOR + nameAndTags.k,
                        fromPostgreSQL(typeName),
                        nameAndTags.v));
              } else {
                columns.add(
                    new Column(
                        databaseName + SEPARATOR + tableName + SEPARATOR + nameAndTags.k,
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
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectWithFilter(project, filter, dataArea);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectDummyWithFilter(project, filter);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectWithFilter(project, select.getFilter(), dataArea);
  }

  private TaskExecuteResult executeProjectWithFilter(
      Project project, Filter filter, DataArea dataArea) {
    try {
      String databaseName = dataArea.getStorageUnit();
      Connection conn = getConnection(databaseName);
      if (conn == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      Statement stmt;

      Map<String, String> tableNameToColumnNames =
          splitAndMergeQueryPatterns(databaseName, conn, project.getPatterns());

      String statement;
      // 如果table>1的情况下存在Value或Path Filter，说明filter的匹配需要跨table，此时需要将所有table join到一起进行查询
      if (!filter.toString().contains("*")
          && !(tableNameToColumnNames.size() > 1
              && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
        for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
          String tableName = entry.getKey();
          String quotColumnNames = getQuotColumnNames(entry.getValue());
          statement =
              String.format(
                  QUERY_STATEMENT,
                  quotColumnNames,
                  getQuotName(tableName),
                  FilterTransformer.toString(filter));

          ResultSet rs = null;
          try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(statement);
            logger.info("[Query] execute query: {}", statement);
          } catch (SQLException e) {
            logger.error("meet error when executing query {}: {}", statement, e.getMessage());
            continue;
          }
          if (rs != null) {
            databaseNameList.add(databaseName);
            resultSets.add(rs);
          }
        }
      }
      // table中带有了通配符，将所有table都join到一起进行查询，以便输入filter.
      else if (!tableNameToColumnNames.isEmpty()) {
        List<String> tableNames = new ArrayList<>();
        List<List<String>> fullColumnNamesList = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
          String tableName = entry.getKey();
          tableNames.add(tableName);
          List<String> fullColumnNames =
              new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));

          // 将columnNames中的列名加上tableName前缀
          fullColumnNames.replaceAll(s -> PostgreSQLSchema.getQuotFullName(tableName, s));
          fullColumnNamesList.add(fullColumnNames);
        }

        StringBuilder fullColumnNames = new StringBuilder();
        fullColumnNames.append(PostgreSQLSchema.getQuotFullName(tableNames.get(0), KEY_NAME));
        for (List<String> columnNames : fullColumnNamesList) {
          for (String columnName : columnNames) {
            fullColumnNames.append(", ").append(columnName);
          }
        }

        // table之间用FULL OUTER JOIN ON table1.⺅= table2.⺅ 连接，超过2个table的情况下，需要多次嵌套join
        StringBuilder fullTableName = new StringBuilder();
        fullTableName.append(getQuotName(tableNames.get(0)));
        for (int i = 1; i < tableNames.size(); i++) {
          fullTableName.insert(0, "(");
          fullTableName
              .append(" FULL OUTER JOIN ")
              .append(getQuotName(tableNames.get(i)))
              .append(" ON ");
          for (int j = 0; j < i; j++) {
            fullTableName
                .append(PostgreSQLSchema.getQuotFullName(tableNames.get(i), KEY_NAME))
                .append(" = ")
                .append(PostgreSQLSchema.getQuotFullName(tableNames.get(j), KEY_NAME));

            if (j != i - 1) {
              fullTableName.append(" AND ");
            }
          }
          fullTableName.append(")");
        }

        // 对通配符做处理，将通配符替换成对应的列名
        if (FilterTransformer.toString(filter).contains("*")) {
          filter = generateWildCardsFilter(filter, fullColumnNamesList);
          filter = ExprUtils.mergeTrue(filter);
        }

        statement =
            String.format(
                QUERY_STATEMENT_WITHOUT_KEYNAME,
                fullColumnNames,
                fullTableName,
                FilterTransformer.toString(filter),
                PostgreSQLSchema.getQuotFullName(tableNames.get(0), KEY_NAME));

        ResultSet rs = null;
        try {
          stmt = conn.createStatement();
          rs = stmt.executeQuery(statement);
          logger.info("[Query] execute query: {}", statement);
        } catch (SQLException e) {
          logger.error("meet error when executing query {}: {}", statement, e.getMessage());
        }
        if (rs != null) {
          databaseNameList.add(databaseName);
          resultSets.add(rs);
        }
      }

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new PostgreSQLQueryRowStream(
                  databaseNameList, resultSets, false, filter, project.getTagFilter()));
      conn.close();
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in postgresql failure", e));
    }
  }

  private Filter generateWildCardsFilter(Filter filter, List<List<String>> columnNamesList) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = generateWildCardsFilter(child, columnNamesList);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = generateWildCardsFilter(child, columnNamesList);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = generateWildCardsFilter(notChild, columnNamesList);
        return new NotFilter(newFilter);
      case Value:
        String path = ((ValueFilter) filter).getPath();
        if (path.contains("*")) {
          List<String> matchedPath = getMatchedPath(path, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new ValueFilter(
                matchedPath.get(0),
                ((ValueFilter) filter).getOp(),
                ((ValueFilter) filter).getValue());
          } else {
            List<Filter> andValueChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              andValueChildren.add(
                  new ValueFilter(
                      matched, ((ValueFilter) filter).getOp(), ((ValueFilter) filter).getValue()));
            }
            return new AndFilter(andValueChildren);
          }
        }

        return filter;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        if (pathA.contains("*")) {
          List<String> matchedPath = getMatchedPath(pathA, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new PathFilter(
                matchedPath.get(0),
                ((PathFilter) filter).getOp(),
                ((PathFilter) filter).getPathB());
          } else {
            List<Filter> andPathChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              andPathChildren.add(
                  new PathFilter(
                      matched, ((PathFilter) filter).getOp(), ((PathFilter) filter).getPathB()));
            }
            filter = new AndFilter(andPathChildren);
          }
        }

        if (pathB.contains("*")) {
          if (filter.getType() != FilterType.And) {
            return generateWildCardsFilter(filter, columnNamesList);
          }

          List<String> matchedPath = getMatchedPath(pathB, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new PathFilter(
                ((PathFilter) filter).getPathA(),
                ((PathFilter) filter).getOp(),
                matchedPath.get(0));
          } else {
            List<Filter> andPathChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              andPathChildren.add(
                  new PathFilter(
                      ((PathFilter) filter).getPathA(), ((PathFilter) filter).getOp(), matched));
            }
            return new AndFilter(andPathChildren);
          }
        }

        return filter;

      case Bool:
      case Key:
      default:
        break;
    }
    return filter;
  }

  private List<String> getMatchedPath(String path, List<List<String>> columnNamesList) {
    List<String> matchedPath = new ArrayList<>();
    path = path.replaceAll("[.^${}]", "\\\\$0");
    path = path.replace("*", ".*");
    Pattern pattern = Pattern.compile("^" + path + "$");
    for (int i = 0; i < columnNamesList.size(); i++) {
      List<String> columnNames = columnNamesList.get(i);
      for (String columnName : columnNames) {
        Matcher matcher = pattern.matcher(columnName);
        if (matcher.find()) {
          matchedPath.add(columnName);
        }
      }
    }
    return matchedPath;
  }

  private Filter cutFilterDatabaseNameForDummy(Filter filter, String databaseName) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        andChildren.replaceAll(child -> cutFilterDatabaseNameForDummy(child, databaseName));
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        orChildren.replaceAll(child -> cutFilterDatabaseNameForDummy(child, databaseName));
        return new OrFilter(orChildren);
      case Not:
        return new NotFilter(
            cutFilterDatabaseNameForDummy(((NotFilter) filter).getChild(), databaseName));
      case Value:
        String path = ((ValueFilter) filter).getPath();
        if (path.startsWith(databaseName + SEPARATOR)) {
          return new ValueFilter(
              path.substring(databaseName.length() + 1),
              ((ValueFilter) filter).getOp(),
              ((ValueFilter) filter).getValue());
        }
        break;
      case Path:
        boolean isChanged = false;
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        if (pathA.startsWith(databaseName + SEPARATOR)) {
          pathA = pathA.substring(databaseName.length() + 1);
          isChanged = true;
        }
        if (pathB.startsWith(databaseName + SEPARATOR)) {
          pathB = pathB.substring(databaseName.length() + 1);
          isChanged = true;
        }
        if (isChanged) {
          return new PathFilter(pathA, ((PathFilter) filter).getOp(), pathB);
        }
        break;
      default:
        break;
    }
    return filter;
  }

  private Map<String, String> getAllColumnNameForTable(
      String databaseName, Map<String, String> tableNameToColumnNames) throws SQLException {
    Map<String, String> allColumnNameForTable = new HashMap<>();
    for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
      String tableName = entry.getKey();
      String columnNames = "";
      Connection conn = getConnection(databaseName);
      if (conn == null) {
        continue;
      }
      ResultSet rs = conn.getMetaData().getColumns(databaseName, "public", tableName, "%");
      while (rs.next()) {
        tableName = rs.getString("TABLE_NAME");
        columnNames = PostgreSQLSchema.getQuotFullName(tableName, rs.getString("COLUMN_NAME"));
        if (allColumnNameForTable.containsKey(tableName)) {
          columnNames = allColumnNameForTable.get(tableName) + ", " + columnNames;
        }
        allColumnNameForTable.put(tableName, columnNames);
      }
    }
    return allColumnNameForTable;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    Filter filter = select.getFilter();
    return executeProjectDummyWithFilter(project, filter);
  }

  private TaskExecuteResult executeProjectDummyWithFilter(Project project, Filter filter) {
    try {
      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      ResultSet rs = null;
      Connection conn = null;
      Statement stmt;
      String statement;

      Map<String, Map<String, String>> splitResults =
          splitAndMergeHistoryQueryPatterns(project.getPatterns());
      for (Map.Entry<String, Map<String, String>> splitEntry : splitResults.entrySet()) {
        Map<String, String> tableNameToColumnNames = splitEntry.getValue();
        String databaseName = splitEntry.getKey();
        conn = getConnection(databaseName);
        if (conn == null) {
          continue;
        }
        if (!filter.toString().contains("*")
            && !(tableNameToColumnNames.size() > 1
                && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
          Map<String, String> allColumnNameForTable =
              getAllColumnNameForTable(databaseName, tableNameToColumnNames);
          for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
            String tableName = entry.getKey();
            String fullQuotColumnNames = getQuotColumnNames(entry.getValue());
            List<String> fullPathList = Arrays.asList(entry.getValue().split(", "));
            fullPathList.replaceAll(s -> PostgreSQLSchema.getQuotFullName(tableName, s));
            statement =
                String.format(
                    CONCAT_QUERY_STATEMENT_WITH_WHERE_CLAUSE_AND_CONCAT_KEY,
                    allColumnNameForTable.get(tableName),
                    fullQuotColumnNames,
                    getQuotName(tableName),
                    FilterTransformer.toString(
                        dummyFilterSetTrueByColumnNames(
                            cutFilterDatabaseNameForDummy(filter.copy(), databaseName),
                            fullPathList)),
                    allColumnNameForTable.get(tableName));

            try {
              stmt = conn.createStatement();
              rs = stmt.executeQuery(statement);
              logger.info("[Query] execute query: {}", statement);
            } catch (SQLException e) {
              logger.error("meet error when executing query {}: {}", statement, e.getMessage());
              continue;
            }
            databaseNameList.add(databaseName);
            resultSets.add(rs);
          }
        }
        // table中带有了通配符，将所有table都join到一起进行查询，以便输入filter.
        else if (!tableNameToColumnNames.isEmpty()) {
          List<String> tableNames = new ArrayList<>();
          List<List<String>> fullColumnNamesList = new ArrayList<>();
          List<List<String>> fullQuotColumnNamesList = new ArrayList<>();

          // 将columnNames中的列名加上tableName前缀，带JOIN的查询语句中需要用到
          for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
            String tableName = entry.getKey();
            tableNames.add(tableName);
            List<String> columnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            columnNames.replaceAll(s -> PostgreSQLSchema.getFullName(tableName, s));
            fullColumnNamesList.add(columnNames);

            List<String> fullColumnNames =
                new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            fullColumnNames.replaceAll(s -> PostgreSQLSchema.getQuotFullName(tableName, s));
            fullQuotColumnNamesList.add(fullColumnNames);
          }

          StringBuilder fullColumnNames = new StringBuilder();
          for (List<String> columnNames : fullQuotColumnNamesList) {
            for (String columnName : columnNames) {
              fullColumnNames.append(columnName).append(", ");
            }
          }
          fullColumnNames.delete(fullColumnNames.length() - 2, fullColumnNames.length());

          // 这里获取所有table的所有列名，用于concat时生成key列。
          Map<String, String> allColumnNameForTable =
              getAllColumnNameForTable(databaseName, tableNameToColumnNames);
          StringBuilder allColumnNames = new StringBuilder();
          for (String tableName : tableNames) {
            allColumnNames.append(allColumnNameForTable.get(tableName)).append(", ");
          }
          allColumnNames.delete(allColumnNames.length() - 2, allColumnNames.length());

          // table之间用FULL OUTER JOIN ON concat(table1所有列) = concat(table2所有列)
          // 连接，超过2个table的情况下，需要多次嵌套join
          StringBuilder fullTableName = new StringBuilder();
          fullTableName.append(tableNames.get(0));
          for (int i = 1; i < tableNames.size(); i++) {
            fullTableName.insert(0, "(");
            fullTableName.append(" FULL OUTER JOIN ").append(tableNames.get(i)).append(" ON ");
            for (int j = 0; j < i; j++) {
              fullTableName
                  .append("CONCAT(")
                  .append(allColumnNameForTable.get(tableNames.get(i)))
                  .append(")")
                  .append(" = ")
                  .append("CONCAT(")
                  .append(allColumnNameForTable.get(tableNames.get(j)))
                  .append(")");
              if (j != i - 1) {
                fullTableName.append(" AND ");
              }
            }
            fullTableName.append(")");
          }

          Filter copyFilter = cutFilterDatabaseNameForDummy(filter.copy(), databaseName);

          copyFilter =
              dummyFilterSetTrueByColumnNames(
                  copyFilter,
                  Arrays.asList(fullColumnNames.toString().replace("\"", "").split(", ")));

          // 对通配符做处理，将通配符替换成对应的列名
          if (FilterTransformer.toString(copyFilter).contains("*")) {
            copyFilter = generateWildCardsFilter(copyFilter, fullColumnNamesList);
            copyFilter = ExprUtils.mergeTrue(copyFilter);
          }

          statement =
              String.format(
                  CONCAT_QUERY_STATEMENT_WITH_WHERE_CLAUSE_AND_CONCAT_KEY,
                  allColumnNames,
                  fullColumnNames,
                  fullTableName,
                  FilterTransformer.toString(copyFilter),
                  allColumnNames);

          try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(statement);
            logger.info("[Query] execute query: {}", statement);
          } catch (SQLException e) {
            logger.error("meet error when executing query {}: {}", statement, e.getMessage());
          }
          if (rs != null) {
            databaseNameList.add(databaseName);
            resultSets.add(rs);
          }
        }
      }

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new PostgreSQLQueryRowStream(
                  databaseNameList, resultSets, true, filter, project.getTagFilter()));
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

  private Filter dummyFilterSetTrueByColumnNames(Filter filter, List<String> columnNameList) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        andChildren.replaceAll(child -> dummyFilterSetTrueByColumnNames(child, columnNameList));
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        orChildren.replaceAll(child -> dummyFilterSetTrueByColumnNames(child, columnNameList));
        return new OrFilter(orChildren);
      case Not:
        return new NotFilter(
            dummyFilterSetTrueByColumnNames(((NotFilter) filter).getChild(), columnNameList));
      case Value:
        String path = ((ValueFilter) filter).getPath();
        if (!path.contains("*") && !columnNameList.contains(path)) {
          return new BoolFilter(true);
        }
        break;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        if ((!pathA.contains("*") && !columnNameList.contains(pathA))
            || (!pathB.contains("*") && !columnNameList.contains(pathB))) {
          return new BoolFilter(true);
        }
        break;
      case Key:
        return new BoolFilter(true);
      default:
        break;
    }
    return filter;
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
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
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
                      DROP_COLUMN_STATEMENT, getQuotName(tableName), getQuotName(columnName));
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
          columnSet = databaseMetaData.getColumns(databaseName, "public", tableName, columnName);
          if (columnSet.next()) {
            for (KeyRange keyRange : delete.getKeyRanges()) {
              statement =
                  String.format(
                      UPDATE_STATEMENT,
                      getQuotName(tableName),
                      getQuotName(columnName),
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
          new PhysicalTaskExecuteFailureException("execute delete task in postgresql failure", e));
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
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    ColumnsInterval columnsInterval;
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
            databaseMetaData.getTables(databaseName, "public", "%", new String[] {"TABLE"});
        while (tableSet.next()) {
          String tableName = tableSet.getString("TABLE_NAME"); // 获取表名称
          ResultSet columnSet = databaseMetaData.getColumns(databaseName, "public", tableName, "%");
          StringBuilder columnNames = new StringBuilder();
          while (columnSet.next()) {
            String columnName = columnSet.getString("COLUMN_NAME"); // 获取列名称
            columnNames.append(columnName);
            columnNames.append(", "); // c1, c2, c3,

            String path = databaseName + SEPARATOR + tableName + SEPARATOR + columnName;
            if (dataPrefix != null && !path.startsWith(dataPrefix)) {
              continue;
            }
            paths.add(path);
          }
          columnNames =
              new StringBuilder(columnNames.substring(0, columnNames.length() - 2)); // c1, c2, c3

          // 获取 key 的范围
          String statement =
              String.format(
                  CONCAT_QUERY_STATEMENT,
                  getQuotColumnNames(columnNames.toString()),
                  getQuotName(tableName));
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

    if (paths.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data!");
    }

    if (dataPrefix != null) {
      columnsInterval = new ColumnsInterval(dataPrefix);
    } else {
      columnsInterval =
          new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
    }

    if (minKey == Long.MAX_VALUE) {
      minKey = 0;
    }
    if (maxKey == 0) {
      maxKey = Long.MAX_VALUE - 1;
    }

    return new Pair<>(columnsInterval, new KeyInterval(minKey, maxKey + 1));
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
        if (pattern.split("\\" + SEPARATOR).length == 1) { // REST 查询的路径中可能不含 .
          tableName = pattern;
          columnNames = "%";
        } else {
          PostgreSQLSchema schema = new PostgreSQLSchema(pattern);
          tableName = schema.getTableName();
          columnNames = schema.getColumnName();
          boolean columnEqualsStar = columnNames.startsWith("*");
          boolean tableContainsStar = tableName.contains("*");
          if (columnEqualsStar || tableContainsStar) {
            tableName = tableName.replace('*', '%');
            if (columnEqualsStar) {
              columnNames = columnNames.replaceFirst("\\*", "%"); // 后面tagkv可能还有*，因此只替换第一个
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
        if (columnNames.equals(KEY_NAME)) {
          continue;
        }
        if (tableNameToColumnNames.containsKey(tableName)) {
          columnNames = tableNameToColumnNames.get(tableName) + ", " + columnNames;
          // 此处需要去重
          List<String> columnNamesList =
              new ArrayList<>(Arrays.asList(tableNameToColumnNames.get(tableName).split(", ")));
          List<String> newColumnNamesList = new ArrayList<>(Arrays.asList(columnNames.split(", ")));
          for (String newColumnName : newColumnNamesList) {
            if (!columnNamesList.contains(newColumnName)) {
              columnNamesList.add(newColumnName);
            }
          }

          columnNames = String.join(", ", columnNamesList);
        }
        tableNameToColumnNames.put(tableName, columnNames);
      }
      rs.close();
    }

    return tableNameToColumnNames;
  }

  private Map<String, Map<String, String>> splitAndMergeHistoryQueryPatterns(List<String> patterns)
      throws SQLException {
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
        String[] parts = pattern.split("\\" + SEPARATOR);

        databaseName = parts[0].replace('*', '%');
        if (parts.length == 1) { // 只有一级
          tableName = "%";
          columnNames = "%";
        } else if (parts.length == 2) {
          tableName = "%";
          columnNames = parts[1].equals("*") ? "%" : parts[1];
        } else {
          PostgreSQLSchema schema = new PostgreSQLSchema(pattern, true);
          tableName = schema.getTableName().replace("*", "%");
          columnNames = schema.getColumnName();
          if (columnNames.startsWith("*")) {
            tableName += "%";
            columnNames = columnNames.replaceFirst("\\*", "%");
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
                databaseMetaData.getColumns(tempDatabaseName, "public", tempTableName, columnNames);
            while (columnSet.next()) {
              String tempColumnNames = columnSet.getString("COLUMN_NAME");
              Map<String, String> tableNameToColumnNames = new HashMap<>();
              if (splitResults.containsKey(tempDatabaseName)) {
                tableNameToColumnNames = splitResults.get(tempDatabaseName);
                tempColumnNames =
                    tableNameToColumnNames.get(tempTableName) + ", " + tempColumnNames;
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
            conn.getMetaData().getColumns(databaseName, "public", tableName, columnNames);
        Map<String, String> tableNameToColumnNames = new HashMap<>();
        while (rs.next()) {
          tableName = rs.getString("TABLE_NAME");
          columnNames = rs.getString("COLUMN_NAME");
          if (tableNameToColumnNames.containsKey(tableName)) {
            columnNames = tableNameToColumnNames.get(tableName) + ", " + columnNames;
          }
          tableNameToColumnNames.put(tableName, columnNames);
        }
        if (splitResults.containsKey(databaseName)) {
          Map<String, String> oldTableNameToColumnNames = splitResults.get(databaseName);
          for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
            String oldColumnNames = oldTableNameToColumnNames.get(entry.getKey());
            if (oldColumnNames != null) {
              List<String> oldColumnNameList =
                  Arrays.asList((oldColumnNames + ", " + entry.getValue()).split(", "));
              // 对list去重
              List<String> newColumnNameList = new ArrayList<>();
              for (String columnName : oldColumnNameList) {
                if (!newColumnNameList.contains(columnName)) {
                  newColumnNameList.add(columnName);
                }
              }
              oldTableNameToColumnNames.put(entry.getKey(), String.join(", ", newColumnNameList));
            } else {
              oldTableNameToColumnNames.put(entry.getKey(), entry.getValue());
            }
          }
          tableNameToColumnNames = oldTableNameToColumnNames;
        }
        splitResults.put(databaseName, tableNameToColumnNames);
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
      PostgreSQLSchema schema = new PostgreSQLSchema(path);
      String tableName = schema.getTableName();
      String columnName = schema.getColumnName();

      try {
        Statement stmt = conn.createStatement();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableSet =
            databaseMetaData.getTables(storageUnit, "public", tableName, new String[] {"TABLE"});
        columnName = toFullName(columnName, tags);
        if (!tableSet.next()) {
          String statement =
              String.format(
                  CREATE_TABLE_STATEMENT,
                  getQuotName(tableName),
                  getQuotName(columnName),
                  DataTypeTransformer.toPostgreSQL(dataType));
          logger.info("[Create] execute create: {}", statement);
          stmt.execute(statement);
        } else {
          ResultSet columnSet =
              databaseMetaData.getColumns(storageUnit, "public", tableName, columnName);
          if (!columnSet.next()) {
            String statement =
                String.format(
                    ADD_COLUMN_STATEMENT,
                    getQuotName(tableName),
                    getQuotName(columnName),
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
            "create or alter table {} field {} error: {}", tableName, columnName, e.getMessage());
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
          conn, databaseName, data.getPaths(), data.getTagsList(), data.getDataTypeList());

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
            PostgreSQLSchema schema = new PostgreSQLSchema(path);
            String tableName = schema.getTableName();
            String columnName = schema.getColumnName();
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
                        + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8)
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

  private Exception insertNonAlignedColumnRecords(
      Connection conn, String databaseName, ColumnDataView data) {
    int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
    try {
      Statement stmt = conn.createStatement();

      // 创建表
      createOrAlterTables(
          conn, databaseName, data.getPaths(), data.getTagsList(), data.getDataTypeList());

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
          PostgreSQLSchema schema = new PostgreSQLSchema(path);
          String tableName = schema.getTableName();
          String columnName = schema.getColumnName();
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
                        + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8)
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
    for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
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
      statement.append(getQuotName(tableName));
      statement.append(" (");
      statement.append(getQuotName(KEY_NAME));
      statement.append(", ");
      String fullColumnNames = getQuotColumnNames(columnNames);
      statement.append(fullColumnNames);

      statement.append(") VALUES ");
      for (String value : values) {
        statement.append("(");
        statement.append(value, 0, value.length() - 2);
        statement.append("), ");
      }
      statement = new StringBuilder(statement.substring(0, statement.length() - 2));

      statement.append(" ON CONFLICT (");
      statement.append(getQuotName(KEY_NAME));
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
        statement.append(getQuotName(part));
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
          PostgreSQLSchema schema = new PostgreSQLSchema(fullPath);
          String tableName = schema.getTableName();
          String columnName = toFullName(schema.getColumnName(), column.getTags());
          deletedPaths.add(new Pair<>(tableName, columnName));
          break;
        }
      }
    }

    return deletedPaths;
  }

  private String getQuotName(String name) {
    return "\"" + name + "\"";
    //        return Character.isDigit(name.charAt(0)) ? "\"" + name + "\"" : name;
  }

  private String getQuotColumnNames(String columnNames) {
    String[] parts = columnNames.split(", ");
    StringBuilder fullColumnNames = new StringBuilder();
    for (String part : parts) {
      fullColumnNames.append(getQuotName(part));
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
