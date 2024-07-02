/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.relational;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
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
import cn.edu.tsinghua.iginx.relational.exception.RelationalException;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.meta.JDBCMeta;
import cn.edu.tsinghua.iginx.relational.query.entity.RelationQueryRowStream;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.relational.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.relational.tools.RelationSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationalStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalStorage.class);

  private final StorageEngineMeta meta;

  private final Connection connection;

  private AbstractRelationalMeta relationalMeta;

  private final String engineName;

  private final Map<String, HikariDataSource> connectionPoolMap = new ConcurrentHashMap<>();

  private final FilterTransformer filterTransformer;

  private Connection getConnection(String databaseName) {
    if (databaseName.startsWith("dummy")) {
      return null;
    }

    if (relationalMeta.getSystemDatabaseName().stream().anyMatch(databaseName::equalsIgnoreCase)) {
      return null;
    }

    try {
      Statement stmt = connection.createStatement();
      stmt.execute(String.format(CREATE_DATABASE_STATEMENT, databaseName));
      stmt.close();
    } catch (SQLException ignored) {
    }

    HikariDataSource dataSource = connectionPoolMap.get(databaseName);
    if (dataSource != null) {
      try {
        return dataSource.getConnection();
      } catch (SQLException e) {
        LOGGER.error("Cannot get connection for database {}", databaseName, e);
        dataSource.close();
      }
    }

    try {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(getUrl(databaseName, meta));
      config.setUsername(meta.getExtraParams().get(USERNAME));
      config.setPassword(meta.getExtraParams().get(PASSWORD));
      config.addDataSourceProperty("prepStmtCacheSize", "250");
      config.setLeakDetectionThreshold(2500);
      config.setConnectionTimeout(30000);
      config.setIdleTimeout(10000);
      config.setMaximumPoolSize(10);
      config.setMinimumIdle(1);
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

      HikariDataSource newDataSource = new HikariDataSource(config);
      connectionPoolMap.put(databaseName, newDataSource);
      return newDataSource.getConnection();
    } catch (SQLException e) {
      LOGGER.error("Cannot get connection for database {}", databaseName, e);
      return null;
    }
  }

  private void closeConnection(String databaseName) {
    HikariDataSource dataSource = connectionPoolMap.get(databaseName);
    if (dataSource != null) {
      dataSource.close();
      connectionPoolMap.remove(databaseName);
    }
  }

  protected String getUrl(String databaseName, StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String engine = extraParams.get("engine");
    return String.format("jdbc:%s://%s:%s/%s", engine, meta.getIp(), meta.getPort(), databaseName);
  }

  public RelationalStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    try {
      buildRelationalMeta();
    } catch (RelationalTaskExecuteFailureException e) {
      throw new StorageInitializationException("cannot build relational meta: ", e);
    }
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    filterTransformer = new FilterTransformer(relationalMeta);
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);
    engineName = extraParams.get("engine");
    String connUrl =
        password == null
            ? String.format(
                "jdbc:%s://%s:%s/?user=%s", engineName, meta.getIp(), meta.getPort(), username)
            : String.format(
                "jdbc:%s://%s:%s/?user=%s&password=%s",
                engineName, meta.getIp(), meta.getPort(), username, password);
    try {
      connection = DriverManager.getConnection(connUrl);
      Statement statement = connection.createStatement();
      statement.close();
    } catch (SQLException e) {
      throw new StorageInitializationException(String.format("cannot connect to %s :", meta), e);
    }
  }

  private void buildRelationalMeta() throws RelationalTaskExecuteFailureException {
    String engineName = meta.getExtraParams().get("engine");
    if (classMap.containsKey(engineName)) {
      try {
        Class<?> clazz = Class.forName(classMap.get(engineName));
        relationalMeta =
            (AbstractRelationalMeta)
                clazz.getConstructor(StorageEngineMeta.class).newInstance(meta);
      } catch (Exception e) {
        throw new RelationalTaskExecuteFailureException(
            String.format("engine %s is not supported", engineName), e);
      }
    } else {
      String propertiesPath = meta.getExtraParams().get("meta_properties_path");
      try {
        relationalMeta = new JDBCMeta(meta, propertiesPath);
      } catch (IOException e) {
        throw new RelationalTaskExecuteFailureException(
            String.format("engine %s is not supported", engineName), e);
      }
    }
  }

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);
    String engine = meta.getExtraParams().get("engine");
    String connUrl =
        password == null
            ? String.format(
                "jdbc:%s://%s:%s/?user=%s", engine, meta.getIp(), meta.getPort(), username)
            : String.format(
                "jdbc:%s://%s:%s/?user=%s&password=%s",
                engine, meta.getIp(), meta.getPort(), username, password);

    try {
      Class.forName(relationalMeta.getDriverClass());
      DriverManager.getConnection(connUrl);
      return true;
    } catch (SQLException | ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * 通过JDBC获取ENGINE的所有数据库名称
   *
   * @return 数据库名称列表
   */
  private List<String> getDatabaseNames() throws SQLException {
    List<String> databaseNames = new ArrayList<>();
    Connection conn = getConnection(relationalMeta.getDefaultDatabaseName());
    ResultSet rs = conn.createStatement().executeQuery(relationalMeta.getDatabaseQuerySql());
    while (rs.next()) {
      String databaseName = rs.getString("DATNAME");
      if (relationalMeta.getSystemDatabaseName().contains(databaseName)
          || relationalMeta.getDefaultDatabaseName().equals(databaseName)) {
        continue;
      }
      databaseNames.add(databaseName);
    }
    rs.close();
    conn.close();
    return databaseNames;
  }

  private List<String> getTables(String databaseName, String tablePattern) {
    if (relationalMeta.jdbcNeedQuote()) {
      tablePattern = relationalMeta.getQuote() + tablePattern + relationalMeta.getQuote();
    }
    try {
      Connection conn = getConnection(databaseName);
      if (conn == null) {
        throw new RelationalTaskExecuteFailureException(
            "cannot connect to database " + databaseName);
      }
      DatabaseMetaData databaseMetaData = conn.getMetaData();
      ResultSet rs =
          databaseMetaData.getTables(
              databaseName,
              relationalMeta.getSchemaPattern(),
              tablePattern,
              new String[] {"TABLE"});
      List<String> tableNames = new ArrayList<>();

      while (rs.next()) {
        tableNames.add(rs.getString("TABLE_NAME"));
      }

      rs.close();
      conn.close();
      return tableNames;
    } catch (SQLException | RelationalTaskExecuteFailureException e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
    }
  }

  private List<ColumnField> getColumns(
      String databaseName, String tableName, String columnNamePattern) {
    try {
      Connection conn = getConnection(databaseName);
      if (conn == null) {
        throw new RelationalTaskExecuteFailureException(
            "cannot connect to database " + databaseName);
      }
      DatabaseMetaData databaseMetaData = conn.getMetaData();
      ResultSet rs =
          databaseMetaData.getColumns(
              databaseName, relationalMeta.getSchemaPattern(), tableName, columnNamePattern);
      List<ColumnField> columnFields = new ArrayList<>();
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        String columnType = rs.getString("TYPE_NAME");
        String columnTable = rs.getString("TABLE_NAME");
        columnFields.add(new ColumnField(columnTable, columnName, columnType));
      }
      rs.close();
      conn.close();
      return columnFields;
    } catch (SQLException | RelationalTaskExecuteFailureException e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
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
  public List<Column> getColumns() throws RelationalTaskExecuteFailureException {
    List<Column> columns = new ArrayList<>();
    Map<String, String> extraParams = meta.getExtraParams();
    try {
      for (String databaseName : getDatabaseNames()) {
        if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false"))
            && !databaseName.startsWith(DATABASE_PREFIX)) {
          continue;
        }

        List<String> tables = getTables(databaseName, "%");
        for (String tableName : tables) {
          List<ColumnField> columnFieldList = getColumns(databaseName, tableName, "%");
          for (ColumnField columnField : columnFieldList) {
            String columnName = columnField.columnName;
            String typeName = columnField.columnType;
            if (columnName.equals(KEY_NAME)) { // key 列不显示
              continue;
            }
            Pair<String, Map<String, String>> nameAndTags = splitFullName(columnName);
            if (databaseName.startsWith(DATABASE_PREFIX)) {
              columns.add(
                  new Column(
                      tableName + SEPARATOR + nameAndTags.k,
                      relationalMeta.getDataTypeTransformer().fromEngineType(typeName),
                      nameAndTags.v));
            } else {
              columns.add(
                  new Column(
                      databaseName + SEPARATOR + tableName + SEPARATOR + nameAndTags.k,
                      relationalMeta.getDataTypeTransformer().fromEngineType(typeName),
                      nameAndTags.v));
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new RelationalTaskExecuteFailureException("failed to get columns ", e);
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
            new RelationalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      Statement stmt;

      Map<String, String> tableNameToColumnNames =
          splitAndMergeQueryPatterns(databaseName, project.getPatterns());

      String statement;
      // 如果table>1的情况下存在Value或Path Filter，说明filter的匹配需要跨table，此时需要将所有table join到一起进行查询
      if (!filter.toString().contains("*")
          && !(tableNameToColumnNames.size() > 1
              && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
        for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
          String tableName = entry.getKey();
          String quotColumnNames = getQuotColumnNames(entry.getValue());
          String filterStr = filterTransformer.toString(filter);
          statement =
              String.format(
                  relationalMeta.getQueryStatement(),
                  quotColumnNames,
                  getQuotName(tableName),
                  filterStr.isEmpty() ? "" : "WHERE " + filterStr);

          ResultSet rs = null;
          try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(statement);
            LOGGER.info("[Query] execute query: {}", statement);
          } catch (SQLException e) {
            LOGGER.error("meet error when executing query {}: ", statement, e);
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
          fullColumnNames.replaceAll(
              s -> RelationSchema.getQuoteFullName(tableName, s, relationalMeta.getQuote()));
          fullColumnNamesList.add(fullColumnNames);
        }

        StringBuilder fullColumnNames = new StringBuilder();
        fullColumnNames.append(
            RelationSchema.getQuoteFullName(
                tableNames.get(0), KEY_NAME, relationalMeta.getQuote()));
        for (List<String> columnNames : fullColumnNamesList) {
          for (String columnName : columnNames) {
            fullColumnNames.append(", ").append(columnName);
          }
        }

        // 将所有表进行full join
        String fullTableName = getFullJoinTables(tableNames, fullColumnNamesList);

        // 对通配符做处理，将通配符替换成对应的列名
        if (filterTransformer.toString(filter).contains("*")) {
          // 把fullColumnNamesList中的列名全部用removeFullColumnNameQuote去掉引号
          fullColumnNamesList.replaceAll(
              columnNames -> {
                List<String> newColumnNames = new ArrayList<>();
                for (String columnName : columnNames) {
                  newColumnNames.add(removeFullColumnNameQuote(columnName));
                }
                return newColumnNames;
              });
          filter = generateWildCardsFilter(filter, fullColumnNamesList);
          filter = LogicalFilterUtils.mergeTrue(filter);
        }

        String fullColumnNamesStr = fullColumnNames.toString();
        String filterStr = filterTransformer.toString(filter);
        String orderByKey =
            RelationSchema.getQuoteFullName(tableNames.get(0), KEY_NAME, relationalMeta.getQuote());
        if (!relationalMeta.isSupportFullJoin()) {
          // 如果不支持full join,需要为left join + union模拟的full join表起别名，同时select、where、order by的部分都要调整
          fullColumnNamesStr = fullColumnNamesStr.replaceAll("`\\.`", ".");
          filterStr = filterStr.replaceAll("`\\.`", ".");
          filterStr =
              filterStr.replace(
                  getQuotName(KEY_NAME), getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME));
          orderByKey = orderByKey.replaceAll("`\\.`", ".");
        }
        statement =
            String.format(
                QUERY_STATEMENT_WITHOUT_KEYNAME,
                fullColumnNamesStr,
                fullTableName,
                filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                orderByKey);

        ResultSet rs = null;
        try {
          stmt = conn.createStatement();
          rs = stmt.executeQuery(statement);
          LOGGER.info("[Query] execute query: {}", statement);
        } catch (SQLException e) {
          LOGGER.error("meet error when executing query {}: ", statement, e);
        }
        if (rs != null) {
          databaseNameList.add(databaseName);
          resultSets.add(rs);
        }
      }

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new RelationQueryRowStream(
                  databaseNameList,
                  resultSets,
                  false,
                  filter,
                  project.getTagFilter(),
                  Collections.singletonList(conn),
                  relationalMeta));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute project task in %s failure", engineName), e));
    }
  }

  private String getFullJoinTables(List<String> tableNames, List<List<String>> fullColumnList) {
    StringBuilder fullTableName = new StringBuilder();
    if (relationalMeta.isSupportFullJoin()) {
      // 支持全连接，就直接用全连接连接各个表
      fullTableName.append(getQuotName(tableNames.get(0)));
      for (int i = 1; i < tableNames.size(); i++) {
        fullTableName.insert(0, "(");
        fullTableName
            .append(" FULL OUTER JOIN ")
            .append(getQuotName(tableNames.get(i)))
            .append(" ON ");
        for (int j = 0; j < i; j++) {
          fullTableName
              .append(
                  RelationSchema.getQuoteFullName(
                      tableNames.get(i), KEY_NAME, relationalMeta.getQuote()))
              .append(" = ")
              .append(
                  RelationSchema.getQuoteFullName(
                      tableNames.get(j), KEY_NAME, relationalMeta.getQuote()));

          if (j != i - 1) {
            fullTableName.append(" AND ");
          }
        }
        fullTableName.append(")");
      }
    } else {
      // 不支持全连接，就要用Left Join+Union来模拟全连接
      StringBuilder allColumns = new StringBuilder();
      fullColumnList.forEach(
          columnList ->
              columnList.forEach(
                  column ->
                      allColumns.append(
                          String.format("%s AS %s, ", column, column.replaceAll("`\\.`", ".")))));
      allColumns.delete(allColumns.length() - 2, allColumns.length());

      fullTableName.append("(");
      for (int i = 0; i < tableNames.size(); i++) {

        String keyStr =
            String.format(
                "%s.%s AS %s",
                getQuotName(tableNames.get(i)),
                getQuotName(KEY_NAME),
                getQuotName(tableNames.get(i) + SEPARATOR + KEY_NAME));
        fullTableName.append(
            String.format(
                "SELECT %s FROM %s", keyStr + ", " + allColumns, getQuotName(tableNames.get(i))));
        for (int j = 0; j < tableNames.size(); j++) {
          if (i != j) {
            fullTableName.append(
                String.format(
                    " LEFT JOIN %s ON %s.%s = %s.%s",
                    getQuotName(tableNames.get(j)),
                    getQuotName(tableNames.get(i)),
                    getQuotName(KEY_NAME),
                    getQuotName(tableNames.get(j)),
                    getQuotName(KEY_NAME)));
          }
        }
        if (i != tableNames.size() - 1) {
          fullTableName.append(" UNION ");
        }
      }
      fullTableName.append(")");

      fullTableName.append(" AS derived ");
    }

    return fullTableName.toString();
  }

  /**
   * 通过columnNames构建concat语句，由于concat的参数个数有限，所以需要分批次嵌套concat
   * 由于pg、mysql一张表最大都不能超过2000列，因此嵌套2层，可达到最大10000列，已满足需求。
   *
   * @param columnNames 列名列表
   * @return concat语句
   */
  private String buildConcat(List<String> columnNames) {
    int n = columnNames.size();
    assert n > 0;
    List<List<String>> concatList = new ArrayList<>();
    for (int i = 0; i < n / 100 + 1; i++) {
      List<String> subList = columnNames.subList(i * 100, Math.min((i + 1) * 100, n));
      if (subList.size() == 0) {
        continue;
      }
      concatList.add(subList);
    }

    if (concatList.size() == 1) {
      return String.format(" CONCAT(%s) ", String.join(", ", concatList.get(0)));
    }

    StringBuilder concat = new StringBuilder();
    concat.append(" CONCAT(");
    for (int i = 0; i < concatList.size(); i++) {
      concat.append(String.format(" CONCAT(%s) ", String.join(", ", concatList.get(i))));
      if (i != concatList.size() - 1) {
        concat.append(", ");
      }
    }
    concat.append(") ");
    return concat.toString();
  }

  private String getDummyFullJoinTables(
      List<String> tableNames, Map<String, List<String>> allColumnNameForTable) {
    StringBuilder fullTableName = new StringBuilder();
    if (relationalMeta.isSupportFullJoin()) {
      // table之间用FULL OUTER JOIN ON concat(table1所有列) = concat(table2所有列)
      // 连接，超过2个table的情况下，需要多次嵌套join
      fullTableName.append(getQuotName(tableNames.get(0)));
      for (int i = 1; i < tableNames.size(); i++) {
        fullTableName.insert(0, "(");
        fullTableName
            .append(" FULL OUTER JOIN ")
            .append(getQuotName(tableNames.get(i)))
            .append(" ON ");
        for (int j = 0; j < i; j++) {
          fullTableName.append(
              String.format(
                  "%s = %s",
                  buildConcat(allColumnNameForTable.get(tableNames.get(i))),
                  buildConcat(allColumnNameForTable.get(tableNames.get(j)))));
          if (j != i - 1) {
            fullTableName.append(" AND ");
          }
        }
        fullTableName.append(")");
      }
    } else {
      // 不支持全连接，就要用Left Join+Union来模拟全连接
      char quote = relationalMeta.getQuote();
      String allColumns =
          tableNames.stream()
              .map(allColumnNameForTable::get)
              .flatMap(List::stream)
              .map(s -> s + " AS " + s.replaceAll(quote + "\\." + quote, "."))
              .collect(Collectors.joining(", "));

      fullTableName.append("(");
      for (int i = 0; i < tableNames.size(); i++) {
        String keyStr =
            String.format(
                "%s AS %s",
                getQuotName(KEY_NAME), getQuotName(tableNames.get(i) + SEPARATOR + KEY_NAME));
        fullTableName.append(
            String.format(
                "SELECT %s FROM %s", keyStr + ", " + allColumns, getQuotName(tableNames.get(i))));
        for (int j = 0; j < tableNames.size(); j++) {
          if (i != j) {
            fullTableName.append(
                String.format(
                    " LEFT JOIN %s ON %s = %s",
                    getQuotName(tableNames.get(j)),
                    buildConcat(allColumnNameForTable.get(tableNames.get(i))),
                    buildConcat(allColumnNameForTable.get(tableNames.get(j)))));
          }
        }
        if (i != tableNames.size() - 1) {
          fullTableName.append(" UNION ");
        }
      }
      fullTableName.append(")");
      fullTableName.append(" AS derived ");
    }

    return fullTableName.toString();
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

            if (Op.isOrOp(((ValueFilter) filter).getOp())) {
              return new OrFilter(andValueChildren);
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
            if (Op.isOrOp(((ValueFilter) filter).getOp())) {
              filter = new OrFilter(andPathChildren);
            } else {
              filter = new AndFilter(andPathChildren);
            }
          }
        }

        if (pathB.contains("*")) {
          if (filter.getType() != FilterType.Path) {
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
            if (Op.isOrOp(((ValueFilter) filter).getOp())) {
              return new OrFilter(andPathChildren);
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
    path = StringUtils.reformatPath(path);
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

  private String removeFullColumnNameQuote(String fullColumnName) {
    return fullColumnName
        .substring(1, fullColumnName.length() - 1)
        .replace(relationalMeta.getQuote() + "." + relationalMeta.getQuote(), ".");
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

  private Map<String, List<String>> getAllColumnNameForTable(
      String databaseName, Map<String, String> tableNameToColumnNames) throws SQLException {
    Map<String, List<String>> allColumnNameForTable = new HashMap<>();
    for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
      String tableName = entry.getKey();

      List<ColumnField> columnFieldList = getColumns(databaseName, tableName, "%");
      for (ColumnField columnField : columnFieldList) {
        String columnName = columnField.columnName;

        if (allColumnNameForTable.containsKey(tableName)) {
          allColumnNameForTable
              .get(tableName)
              .add(
                  RelationSchema.getQuoteFullName(
                      tableName, columnName, relationalMeta.getQuote()));
          continue;
        }

        List<String> columnNames = new ArrayList<>();
        columnNames.add(
            RelationSchema.getQuoteFullName(tableName, columnName, relationalMeta.getQuote()));
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
    List<Connection> connList = new ArrayList<>();
    try {
      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      ResultSet rs = null;
      Connection conn = null;
      Statement stmt = null;
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
        connList.add(conn);

        // 这里获取所有table的所有列名，用于concat时生成key列。
        Map<String, List<String>> allColumnNameForTable =
            getAllColumnNameForTable(databaseName, tableNameToColumnNames);

        // 如果table没有带通配符，那直接简单构建起查询语句即可
        if (!filter.toString().contains("*")
            && !(tableNameToColumnNames.size() > 1
                && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {

          for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
            String tableName = entry.getKey();
            String fullQuotColumnNames = getQuotColumnNames(entry.getValue());
            List<String> fullPathList = Arrays.asList(entry.getValue().split(", "));
            fullPathList.replaceAll(
                s -> RelationSchema.getQuoteFullName(tableName, s, relationalMeta.getQuote()));
            String filterStr =
                filterTransformer.toString(
                    dummyFilterSetTrueByColumnNames(
                        cutFilterDatabaseNameForDummy(filter.copy(), databaseName), fullPathList));
            String concatKey = buildConcat(fullPathList);
            statement =
                String.format(
                    relationalMeta.getConcatQueryStatement(),
                    concatKey,
                    fullQuotColumnNames,
                    getQuotName(tableName),
                    filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                    concatKey);

            try {
              stmt = conn.createStatement();
              rs = stmt.executeQuery(statement);
              LOGGER.info("[Query] execute query: {}", statement);
            } catch (SQLException e) {
              LOGGER.error("meet error when executing query {}: ", statement, e);
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
          List<List<String>> fullQuoteColumnNamesList = new ArrayList<>();

          // 将columnNames中的列名加上tableName前缀，带JOIN的查询语句中需要用到
          for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
            String tableName = entry.getKey();
            tableNames.add(tableName);
            List<String> columnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            columnNames.replaceAll(s -> RelationSchema.getFullName(tableName, s));
            fullColumnNamesList.add(columnNames);

            List<String> fullColumnNames =
                new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            fullColumnNames.replaceAll(
                s -> RelationSchema.getQuoteFullName(tableName, s, relationalMeta.getQuote()));
            fullQuoteColumnNamesList.add(fullColumnNames);
          }

          String fullTableName = getDummyFullJoinTables(tableNames, allColumnNameForTable);

          Filter copyFilter =
              dummyFilterSetTrueByColumnNames(
                  cutFilterDatabaseNameForDummy(filter.copy(), databaseName),
                  fullColumnNamesList.stream().flatMap(List::stream).collect(Collectors.toList()));

          // 对通配符做处理，将通配符替换成对应的列名
          if (filterTransformer.toString(copyFilter).contains("*")) {
            // 把fullColumnNamesList中的列名全部用removeFullColumnNameQuote去掉引号
            fullColumnNamesList.replaceAll(
                columnNames -> {
                  List<String> newColumnNames = new ArrayList<>();
                  for (String columnName : columnNames) {
                    newColumnNames.add(removeFullColumnNameQuote(columnName));
                  }
                  return newColumnNames;
                });
            copyFilter = generateWildCardsFilter(copyFilter, fullColumnNamesList);
            copyFilter = LogicalFilterUtils.mergeTrue(copyFilter);
          }

          String filterStr = filterTransformer.toString(copyFilter);
          String orderByKey =
              buildConcat(
                  fullQuoteColumnNamesList.stream()
                      .flatMap(List::stream)
                      .collect(Collectors.toList()));
          if (!relationalMeta.isSupportFullJoin()) {
            // 如果不支持full join,需要为left join + union模拟的full join表起别名，同时select、where、order by的部分都要调整
            char quote = relationalMeta.getQuote();
            fullQuoteColumnNamesList.forEach(
                columnNames ->
                    columnNames.replaceAll(s -> s.replaceAll(quote + "\\." + quote, ".")));
            filterStr = filterStr.replaceAll("`\\.`", ".");
            filterStr =
                filterStr.replace(
                    getQuotName(KEY_NAME), getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME));
            orderByKey = getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME);
          }

          statement =
              String.format(
                  relationalMeta.getConcatQueryStatement(),
                  buildConcat(
                      fullQuoteColumnNamesList.stream()
                          .flatMap(List::stream)
                          .collect(Collectors.toList())),
                  fullQuoteColumnNamesList.stream()
                      .flatMap(List::stream)
                      .collect(Collectors.joining(", ")),
                  fullTableName,
                  filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                  orderByKey);

          try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(statement);
            LOGGER.info("[Query] execute query: {}", statement);
          } catch (SQLException e) {
            LOGGER.error("meet error when executing query {}: ", statement, e);
          }
          if (rs != null) {
            databaseNameList.add(databaseName);
            resultSets.add(rs);
          }
        }
      }

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new RelationQueryRowStream(
                  databaseNameList,
                  resultSets,
                  true,
                  filter,
                  project.getTagFilter(),
                  connList,
                  relationalMeta));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute project task in %s failure", engineName), e));
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
            new RelationalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      Statement stmt = conn.createStatement();
      String statement;
      List<String> paths = delete.getPatterns();
      List<Pair<String, String>> deletedPaths; // table name -> column name
      String tableName;
      String columnName;
      List<String> tables;

      if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
          closeConnection(databaseName);
          Connection defaultConn =
              getConnection(relationalMeta.getDefaultDatabaseName()); // 正在使用的数据库无法被删除，因此需要切换到默认数据库
          if (defaultConn != null) {
            stmt = defaultConn.createStatement();
            statement = String.format(relationalMeta.getDropDatabaseStatement(), databaseName);
            LOGGER.info("[Delete] execute delete: {}", statement);
            stmt.execute(statement); // 删除数据库
            stmt.close();
            defaultConn.close();
            return new TaskExecuteResult(null, null);
          } else {
            return new TaskExecuteResult(
                new RelationalTaskExecuteFailureException(
                    String.format(
                        "cannot connect to database %s", relationalMeta.getDefaultDatabaseName()),
                    new SQLException()));
          }
        } else {
          deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
          for (Pair<String, String> pair : deletedPaths) {
            tableName = pair.k;
            columnName = pair.v;
            tables = getTables(databaseName, tableName);
            if (!tables.isEmpty()) {
              statement =
                  String.format(
                      DROP_COLUMN_STATEMENT, getQuotName(tableName), getQuotName(columnName));
              LOGGER.info("[Delete] execute delete: {}", statement);
              try {
                stmt.execute(statement); // 删除列
              } catch (SQLException e) {
                // 可能会出现该列不存在的问题，此时不做处理
              }
            }
          }
        }
      } else {
        deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
        for (Pair<String, String> pair : deletedPaths) {
          tableName = pair.k;
          columnName = pair.v;
          if (!getColumns(databaseName, tableName, columnName).isEmpty()) {
            for (KeyRange keyRange : delete.getKeyRanges()) {
              statement =
                  String.format(
                      relationalMeta.getUpdateStatement(),
                      getQuotName(tableName),
                      getQuotName(columnName),
                      keyRange.getBeginKey(),
                      keyRange.getEndKey());
              LOGGER.info("[Delete] execute delete: {}", statement);
              stmt.execute(statement); // 将目标列的目标范围的值置为空
            }
          }
        }
      }
      stmt.close();
      conn.close();
      return new TaskExecuteResult(null, null);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute delete task in %s failure", engineName), e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    DataView dataView = insert.getData();
    String databaseName = dataArea.getStorageUnit();
    Connection conn = getConnection(databaseName);
    if (conn == null) {
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
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
    try {
      conn.close();
    } catch (SQLException ex) {
      LOGGER.error("encounter error when closing connection: {}", ex.getMessage());
    }
    if (e != null) {
      return new TaskExecuteResult(
          null,
          new RelationalException(
              String.format("execute insert task in %s failure", engineName), e));
    }
    return new TaskExecuteResult(null, null);
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    ColumnsInterval columnsInterval;
    List<String> paths = new ArrayList<>();
    try {
      for (String databaseName : getDatabaseNames()) {
        List<String> tables = getTables(databaseName, "%");
        for (String tableName : tables) {
          List<ColumnField> columnFieldList = getColumns(databaseName, tableName, "%");
          for (ColumnField columnField : columnFieldList) {
            String columnName = columnField.columnName; // 获取列名称

            String path = databaseName + SEPARATOR + tableName + SEPARATOR + columnName;
            if (dataPrefix != null && !path.startsWith(dataPrefix)) {
              continue;
            }
            paths.add(path);
          }
        }
      }
    } catch (SQLException e) {
      LOGGER.error("encounter error when getting boundary of storage: ", e);
    }
    paths.sort(String::compareTo);

    if (paths.isEmpty()) {
      throw new RelationalTaskExecuteFailureException("no data!");
    }

    if (dataPrefix != null) {
      columnsInterval = new ColumnsInterval(dataPrefix);
    } else {
      columnsInterval =
          new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
    }

    return new Pair<>(columnsInterval, KeyInterval.getDefaultKeyInterval());
  }

  private List<Pattern> getRegexPatternByName(
      String tableName, String columnNames, boolean isDummy) {
    // 我们输入例如test%，是希望匹配到test或test.abc这样的表，但是不希望匹配到test1这样的表，但语法不支持，因此在这里做一下过滤
    String tableNameRegex = tableName;
    tableNameRegex = StringUtils.reformatPath(tableNameRegex);
    tableNameRegex = tableNameRegex.replace("%", ".*");
    if (tableNameRegex.endsWith(".*")
        && !tableNameRegex.endsWith(SEPARATOR + ".*")
        && !tableNameRegex.equals(".*")) {
      tableNameRegex = tableNameRegex.substring(0, tableNameRegex.length() - 2);
      tableNameRegex += "(\\" + SEPARATOR + ".*)?";
    }
    Pattern tableNamePattern = Pattern.compile("^" + tableNameRegex + "$");

    String columnNameRegex = columnNames;
    if (isDummy) {
      columnNameRegex = StringUtils.reformatPath(columnNameRegex);
      columnNameRegex = columnNameRegex.replace("%", ".*");
    } else {
      if (columnNames.equals("%")) {
        columnNameRegex = ".*";
      } else {
        // columnNames中只会有一个 %
        columnNameRegex = StringUtils.reformatPath(columnNameRegex);
        columnNameRegex = columnNameRegex.replace("%", "(" + TAGKV_SEPARATOR + ".*)?");
      }
    }
    Pattern columnNamePattern = Pattern.compile("^" + columnNameRegex + "$");

    return Arrays.asList(tableNamePattern, columnNamePattern);
  }

  private Map<String, String> splitAndMergeQueryPatterns(String databaseName, List<String> patterns)
      throws SQLException {
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
          RelationSchema schema = new RelationSchema(pattern, relationalMeta.getQuote());
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

      List<ColumnField> columnFieldList;
      if (relationalMeta.jdbcSupportSpecialChar()) {
        columnFieldList =
            getColumns(databaseName, reformatForJDBC(tableName), reformatForJDBC(columnNames));
      } else {
        columnFieldList = getColumns(databaseName, "%", "%");
      }

      List<Pattern> patternList = getRegexPatternByName(tableName, columnNames, false);
      Pattern tableNamePattern = patternList.get(0), columnNamePattern = patternList.get(1);

      for (ColumnField columnField : columnFieldList) {
        String curTableName = columnField.tableName;
        String curColumnNames = columnField.columnName;
        if (curColumnNames.equals(KEY_NAME)) {
          continue;
        }

        if (!tableNamePattern.matcher(curTableName).find()
            || !columnNamePattern.matcher(curColumnNames).find()) {
          continue;
        }

        if (tableNameToColumnNames.containsKey(curTableName)) {
          curColumnNames = tableNameToColumnNames.get(curTableName) + ", " + curColumnNames;
          // 此处需要去重
          List<String> columnNamesList =
              new ArrayList<>(Arrays.asList(tableNameToColumnNames.get(curTableName).split(", ")));
          List<String> newColumnNamesList =
              new ArrayList<>(Arrays.asList(curColumnNames.split(", ")));
          for (String newColumnName : newColumnNamesList) {
            if (!columnNamesList.contains(newColumnName)) {
              columnNamesList.add(newColumnName);
            }
          }

          curColumnNames = String.join(", ", columnNamesList);
        }
        tableNameToColumnNames.put(curTableName, curColumnNames);
      }
    }

    return tableNameToColumnNames;
  }

  /** JDBC中的路径中的 . 不需要转义 */
  private String reformatForJDBC(String path) {
    return StringUtils.reformatPath(path).replace("\\.", ".");
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
          RelationSchema schema = new RelationSchema(pattern, true, relationalMeta.getQuote());
          tableName = schema.getTableName().replace("*", "%");
          columnNames = schema.getColumnName();
          if (columnNames.startsWith("*")) {
            tableName += "%";
            columnNames = columnNames.replaceFirst("\\*", "%");
          }
        }
      }

      List<Pattern> patternList = getRegexPatternByName(tableName, columnNames, true);
      Pattern tableNamePattern = patternList.get(0), columnNamePattern = patternList.get(1);

      if (databaseName.equals("%")) {
        for (String tempDatabaseName : getDatabaseNames()) {
          if (tempDatabaseName.startsWith(DATABASE_PREFIX)) {
            continue;
          }
          List<String> tables = getTables(tempDatabaseName, tableName);
          for (String tempTableName : tables) {
            if (!tableNamePattern.matcher(tempTableName).find()) {
              continue;
            }
            List<ColumnField> columnFieldList =
                getColumns(tempDatabaseName, tempTableName, columnNames);
            for (ColumnField columnField : columnFieldList) {
              String tempColumnNames = columnField.columnName;
              if (!columnNamePattern.matcher(tempColumnNames).find()) {
                continue;
              }
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
        List<ColumnField> columnFieldList = getColumns(databaseName, tableName, columnNames);
        Map<String, String> tableNameToColumnNames = new HashMap<>();
        for (ColumnField columnField : columnFieldList) {
          tableName = columnField.tableName;
          columnNames = columnField.columnName;
          if (!tableNamePattern.matcher(tableName).find()
              || !columnNamePattern.matcher(columnNames).find()) {
            continue;
          }
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
      RelationSchema schema = new RelationSchema(path, relationalMeta.getQuote());
      String tableName = schema.getTableName();
      String columnName = schema.getColumnName();

      try {
        Statement stmt = conn.createStatement();

        List<String> tables = getTables(storageUnit, tableName);
        columnName = toFullName(columnName, tags);
        if (tables.isEmpty()) {
          String statement =
              String.format(
                  relationalMeta.getCreateTableStatement(),
                  getQuotName(tableName),
                  getQuotName(columnName),
                  relationalMeta.getDataTypeTransformer().toEngineType(dataType));
          LOGGER.info("[Create] execute create: {}", statement);
          stmt.execute(statement);
        } else {
          if (getColumns(storageUnit, tableName, columnName).isEmpty()) {
            String statement =
                String.format(
                    ADD_COLUMN_STATEMENT,
                    getQuotName(tableName),
                    getQuotName(columnName),
                    relationalMeta.getDataTypeTransformer().toEngineType(dataType));
            LOGGER.info("[Create] execute create: {}", statement);
            stmt.execute(statement);
          }
        }
        stmt.close();
      } catch (SQLException e) {
        LOGGER.error("create or alter table {} field {} error: ", tableName, columnName, e);
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
            RelationSchema schema = new RelationSchema(path, relationalMeta.getQuote());
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
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
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
          RelationSchema schema = new RelationSchema(path, relationalMeta.getQuote());
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
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
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

      // INSERT INTO XXX ("key", XXX, ...) VALUES (XXX, XXX, ...), (XXX, XXX, ...), ...,
      // (XXX,
      // XXX, ...) ON CONFLICT ("key") DO UPDATE SET (XXX, ...) = (excluded.XXX, ...);
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
      statement.delete(statement.length() - 2, statement.length());

      statement.append(relationalMeta.getUpsertStatement());

      for (String part : parts) {
        if (part.equals(KEY_NAME)) {
          continue;
        }
        statement.append(
            String.format(
                relationalMeta.getUpsertConflictStatement(), getQuotName(part), getQuotName(part)));
        statement.append(", ");
      }

      statement.delete(statement.length() - 2, statement.length());

      statement.append(";");

      stmt.addBatch(statement.toString());
    }
    stmt.executeBatch();
  }

  private List<Pair<String, String>> determineDeletedPaths(
      List<String> paths, TagFilter tagFilter) {
    try {
      List<Column> columns = getColumns();
      List<Pair<String, String>> deletedPaths = new ArrayList<>();

      for (Column column : columns) {
        for (String path : paths) {
          if (Pattern.matches(StringUtils.reformatPath(path), column.getPath())) {
            if (tagFilter != null && !TagKVUtils.match(column.getTags(), tagFilter)) {
              continue;
            }
            String fullPath = column.getPath();
            RelationSchema schema = new RelationSchema(fullPath, relationalMeta.getQuote());
            String tableName = schema.getTableName();
            String columnName = toFullName(schema.getColumnName(), column.getTags());
            deletedPaths.add(new Pair<>(tableName, columnName));
            break;
          }
        }
      }
      return deletedPaths;
    } catch (RelationalTaskExecuteFailureException e) {
      LOGGER.error(e.getMessage(), e);
      return new ArrayList<>();
    }
  }

  private String getQuotName(String name) {
    return relationalMeta.getQuote() + name + relationalMeta.getQuote();
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
      throw new RelationalException(e);
    }
  }
}
