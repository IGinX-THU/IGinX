/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.relational;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.system.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.exception.RelationalException;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.meta.JDBCMeta;
import cn.edu.tsinghua.iginx.relational.query.entity.RelationQueryRowStream;
import cn.edu.tsinghua.iginx.relational.strategy.DatabaseStrategy;
import cn.edu.tsinghua.iginx.relational.strategy.DatabaseStrategyFactory;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.relational.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.relational.tools.QuoteBaseExpressionDecorator;
import cn.edu.tsinghua.iginx.relational.tools.RelationSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import io.reactivex.rxjava3.core.Flowable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

  private final DatabaseStrategy dbStrategy;

  private final ExecutorService executor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("RelationalStorage-%d").build());

  private static final Set<String> SUPPORTED_AGGREGATE_FUNCTIONS =
      new HashSet<>(Arrays.asList(Count.COUNT, Sum.SUM, Avg.AVG, Max.MAX, Min.MIN));

  private Connection getConnection(String databaseName) {
    if (databaseName.startsWith("dummy")) {
      return null;
    }

    if (relationalMeta.getSystemDatabaseName().stream().anyMatch(databaseName::equalsIgnoreCase)) {
      return null;
    }

    if (relationalMeta.supportCreateDatabase()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(
            String.format(relationalMeta.getCreateDatabaseStatement(), getQuotName(databaseName)));
      } catch (SQLException ignored) {
      }
    } else {
      if (databaseName.equals(relationalMeta.getDefaultDatabaseName())
          || databaseName.matches("unit\\d{10}")) {
        databaseName = "";
      }
    }

    HikariDataSource dataSource = connectionPoolMap.get(databaseName);
    if (dataSource != null) {
      try {
        Connection conn;
        conn = dataSource.getConnection();
        return conn;
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
      config.addDataSourceProperty(
          "prepStmtCacheSize", meta.getExtraParams().getOrDefault("prep_stmt_cache_size", "250"));
      config.setLeakDetectionThreshold(
          Long.parseLong(meta.getExtraParams().getOrDefault("leak_detection_threshold", "2500")));
      config.setConnectionTimeout(
          Long.parseLong(meta.getExtraParams().getOrDefault("connection_timeout", "30000")));
      config.setIdleTimeout(
          Long.parseLong(meta.getExtraParams().getOrDefault("idle_timeout", "10000")));
      config.setMaximumPoolSize(
          Integer.parseInt(meta.getExtraParams().getOrDefault("maximum_pool_size", "20")));
      config.setMinimumIdle(
          Integer.parseInt(meta.getExtraParams().getOrDefault("minimum_idle", "1")));
      config.addDataSourceProperty(
          "prepStmtCacheSqlLimit",
          meta.getExtraParams().getOrDefault("prep_stmt_cache_sql_limit", "2048"));
      dbStrategy.configureDataSource(config, databaseName, meta);

      HikariDataSource newDataSource = new HikariDataSource(config);
      connectionPoolMap.put(databaseName, newDataSource);
      return newDataSource.getConnection();
    } catch (SQLException | HikariPool.PoolInitializationException e) {
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
    return dbStrategy.getUrl(databaseName, meta);
  }

  public String getConnectUrl() {
    return dbStrategy.getConnectUrl(meta);
  }

  public RelationalStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    try {
      buildRelationalMeta();
    } catch (RelationalTaskExecuteFailureException e) {
      throw new StorageInitializationException("cannot build relational meta: ", e);
    }
    engineName = meta.getExtraParams().get("engine");
    dbStrategy = DatabaseStrategyFactory.getStrategy(engineName, relationalMeta, meta);
    if (!testConnection(this.meta)) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    filterTransformer = new FilterTransformer(relationalMeta);
    try {
      connection = DriverManager.getConnection(getConnectUrl());
      Statement statement = connection.createStatement();
      statement.close();
    } catch (SQLException e) {
      throw new StorageInitializationException(String.format("cannot connect to %s :", meta), e);
    }
  }

  private void buildRelationalMeta() throws RelationalTaskExecuteFailureException {
    String engineName = meta.getExtraParams().get("engine");
    if (engineName == null) {
      throw new RelationalTaskExecuteFailureException("engine name is not provided");
    }
    // load jdbc meta from properties file
    String propertiesPath = meta.getExtraParams().get("meta_properties_path");
    try {
      Properties properties = getProperties(engineName, propertiesPath);
      relationalMeta = new JDBCMeta(meta, properties);
    } catch (IOException | URISyntaxException e) {
      throw new RelationalTaskExecuteFailureException("failed to load meta properties", e);
    }
  }

  private Properties getProperties(String engine, @Nullable String propertiesPath)
      throws URISyntaxException, IOException {
    if (propertiesPath != null) {
      try (InputStream propertiesIS = Files.newInputStream(Paths.get(propertiesPath))) {
        Properties properties = new Properties();
        properties.load(propertiesIS);
        return properties;
      } catch (IOException e) {
        LOGGER.warn("failed to load properties from path: {}", propertiesPath, e);
      }
    }

    String metaFileName = engine.toLowerCase() + META_TEMPLATE_SUFFIX;
    LOGGER.info("loading engine '{}' default properties from class path: {}", engine, metaFileName);
    URL url = getClass().getClassLoader().getResource(metaFileName);
    if (url == null) {
      throw new IOException("cannot find default meta properties file: " + metaFileName);
    }
    try (InputStream propertiesIS = url.openStream()) {
      Properties properties = new Properties();
      properties.load(propertiesIS);
      return properties;
    }
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    try {
      Class.forName(relationalMeta.getDriverClass());
      DriverManager.getConnection(getConnectUrl());
      return true;
    } catch (SQLException | ClassNotFoundException e) {
      LOGGER.error("Cannot connect to {}", meta, e);
      return false;
    }
  }

  /**
   * 通过JDBC获取ENGINE的所有数据库名称
   *
   * @return 数据库名称列表
   */
  private List<String> getDatabaseNames(boolean isDummy) throws SQLException {
    List<String> databaseNames = new ArrayList<>();
    String DefaultDatabaseName = relationalMeta.getDefaultDatabaseName();
    String query =
        (isDummy || relationalMeta.supportCreateDatabase())
            ? relationalMeta.getDummyDatabaseQuerySql()
            : relationalMeta.getDatabaseQuerySql();
    try (Connection conn = getConnection(DefaultDatabaseName);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        String databaseName = dbStrategy.getDatabaseNameFromResultSet(rs);
        if (relationalMeta.getSystemDatabaseName().contains(databaseName)
            || relationalMeta.getDefaultDatabaseName().equals(databaseName)) {
          continue;
        }
        databaseNames.add(databaseName);
      }
    }
    return databaseNames;
  }

  private List<String> getTables(String databaseName, String tablePattern, boolean isDummy) {
    String databasePattern = dbStrategy.getDatabasePattern(databaseName, isDummy);
    String schemaPattern = dbStrategy.getSchemaPattern(databaseName, isDummy);
    if (!isDummy) {
      tablePattern = reshapeTableNameBeforeQuery(tablePattern, databaseName);
    }
    if (relationalMeta.jdbcNeedQuote()) {
      tablePattern = relationalMeta.getQuote() + tablePattern + relationalMeta.getQuote();
    }
    try (Connection conn = getConnection(databaseName)) {
      if (conn == null) {
        throw new RelationalTaskExecuteFailureException(
            "cannot connect to database " + databaseName);
      }
      DatabaseMetaData databaseMetaData = conn.getMetaData();
      List<String> tableNames = new ArrayList<>();
      try (ResultSet rs =
          databaseMetaData.getTables(
              databasePattern, schemaPattern, tablePattern, new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (!isDummy) {
            tableName = reshapeTableNameAfterQuery(tableName, databaseName);
          }
          tableNames.add(tableName);
        }
      }
      return tableNames;
    } catch (SQLException | RelationalTaskExecuteFailureException e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
    }
  }

  private List<ColumnField> getColumns(
      String databaseName, String tableName, String columnNamePattern, boolean isDummy) {
    String databasePattern = dbStrategy.getDatabasePattern(databaseName, isDummy);
    String schemaPattern = dbStrategy.getSchemaPattern(databaseName, isDummy);
    if (!isDummy) {
      tableName = reshapeTableNameBeforeQuery(tableName, databaseName);
    }
    try (Connection conn = getConnection(databaseName)) {
      if (conn == null) {
        throw new RelationalTaskExecuteFailureException(
            "cannot connect to database " + databaseName);
      }
      List<ColumnField> columnFields = new ArrayList<>();
      DatabaseMetaData databaseMetaData = conn.getMetaData();
      try (ResultSet rs =
          databaseMetaData.getColumns(
              databasePattern, schemaPattern, tableName, columnNamePattern)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String columnType = rs.getString("TYPE_NAME");
          String columnTable = rs.getString("TABLE_NAME");
          if (!isDummy) {
            columnTable = reshapeTableNameAfterQuery(columnTable, databaseName);
          }
          int columnSize = rs.getInt("COLUMN_SIZE");
          int decimalDigits = rs.getInt("DECIMAL_DIGITS");
          columnFields.add(
              new ColumnField(columnTable, columnName, columnType, columnSize, decimalDigits));
        }
      }
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
  public Flowable<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws RelationalTaskExecuteFailureException {
    List<Column> columns = new ArrayList<>();
    Map<String, String> extraParams = meta.getExtraParams();
    try {
      String colPattern;
      // non-dummy
      for (String databaseName : getDatabaseNames(false)) {
        if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false"))
            && !databaseName.startsWith(DATABASE_PREFIX)) {
          continue;
        }
        boolean isDummy =
            extraParams.get("has_data") != null
                && extraParams.get("has_data").equalsIgnoreCase("true")
                && !databaseName.startsWith(DATABASE_PREFIX);
        if (isDummy) {
          continue;
        }

        Map<String, String> tableAndColPattern = new HashMap<>();

        if (patterns != null && patterns.size() != 0) {
          tableAndColPattern = splitAndMergeQueryPatterns(databaseName, new ArrayList<>(patterns));
        } else {
          for (String table : getTables(databaseName, "%", false)) {
            tableAndColPattern.put(table, "%");
          }
        }
        for (String tableName : tableAndColPattern.keySet()) {
          colPattern = tableAndColPattern.get(tableName);
          for (String colName : colPattern.split(", ")) {
            List<ColumnField> columnFieldList = getColumns(databaseName, tableName, colName, false);
            for (ColumnField columnField : columnFieldList) {
              String columnName = columnField.getColumnName();

              if (columnName.equals(KEY_NAME)) { // key 列不显示
                continue;
              }
              Pair<String, Map<String, String>> nameAndTags = splitFullName(columnName);
              columnName = tableName + SEPARATOR + nameAndTags.k;
              if (tagFilter != null && !TagKVUtils.match(nameAndTags.v, tagFilter)) {
                continue;
              }
              columns.add(
                  new Column(
                      columnName,
                      relationalMeta
                          .getDataTypeTransformer()
                          .fromEngineType(
                              columnField.getColumnType(),
                              columnField.getColumnSize(),
                              columnField.getDecimalDigits()),
                      nameAndTags.v,
                      isDummy));
            }
          }
        }
      }

      // dummy
      List<String> patternList = new ArrayList<>();
      for (String databaseName : getDatabaseNames(true)) {
        if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false"))
            && !databaseName.startsWith(DATABASE_PREFIX)) {
          continue;
        }
        boolean isDummy =
            extraParams.get("has_data") != null
                && extraParams.get("has_data").equalsIgnoreCase("true")
                && !databaseName.startsWith(DATABASE_PREFIX);
        if (!isDummy) {
          continue;
        }
        // find pattern that match <databaseName>.* to avoid creating databases after.
        if (patterns == null || patterns.size() == 0) {
          patternList.add(databaseName + ".*");
          continue;
        }
        for (String p : patterns) {
          // dummy path starts with <bucketName>.
          if (Pattern.matches(
              StringUtils.reformatPath(p.substring(0, p.indexOf("."))), databaseName)) {
            patternList.add(p);
          }
        }
      }

      Map<String, Map<String, String>> dummyRes = splitAndMergeHistoryQueryPatterns(patternList);
      Map<String, String> table2cols;
      // seemingly there are 4 nested loops, but it's only the consequence of special data structure
      // and reused methods.
      // the loops would not affect complexity
      for (String databaseName : dummyRes.keySet()) {
        table2cols = dummyRes.get(databaseName);
        for (String tableName : table2cols.keySet()) {
          // 对于不支持database创建的数据库，unit前缀的databaseName将会作为表名前缀混入dummy中，需要进行过滤
          if (!relationalMeta.supportCreateDatabase() && tableName.startsWith(DATABASE_PREFIX)) {
            continue;
          }
          colPattern = table2cols.get(tableName);
          for (String colName : colPattern.split(", ")) {
            List<ColumnField> columnFieldList = getColumns(databaseName, tableName, colName, true);
            for (ColumnField columnField : columnFieldList) {
              String columnName = columnField.getColumnName();

              if (columnName.equals(KEY_NAME)) { // key 列不显示
                continue;
              }
              Pair<String, Map<String, String>> nameAndTags = splitFullName(columnName);
              columnName = databaseName + SEPARATOR + tableName + SEPARATOR + nameAndTags.k;
              if (tagFilter != null && !TagKVUtils.match(nameAndTags.v, tagFilter)) {
                continue;
              }
              columns.add(
                  new Column(
                      columnName,
                      relationalMeta
                          .getDataTypeTransformer()
                          .fromEngineType(
                              columnField.getColumnType(),
                              columnField.getColumnSize(),
                              columnField.getDecimalDigits()),
                      nameAndTags.v,
                      true));
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new RelationalTaskExecuteFailureException("failed to get columns ", e);
    }
    return Flowable.fromIterable(columns);
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

  // TODO: getProjectWithFilterSQL 存在bug，多表 full-join 时，key可能为 null，造成结果不完整

  /** 获取ProjectWithFilter中将所有table join到一起进行查询的SQL语句 */
  private String getProjectWithFilterSQL(
      String databaseName,
      Filter filter,
      Map<String, String> tableNameToColumnNames,
      boolean isAgg) {
    List<String> tableNames = new ArrayList<>();
    List<List<String>> fullColumnNamesList = new ArrayList<>();
    List<List<String>> fullColumnNamesListForExpandFilter = new ArrayList<>();
    String firstTable = "";
    char quote = relationalMeta.getQuote();
    for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
      String tableName = reshapeTableNameBeforeQuery(entry.getKey(), databaseName);
      if (firstTable.isEmpty()) {
        firstTable = tableName;
      }
      tableNames.add(tableName);
      List<String> fullColumnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));

      // 将columnNames中的列名加上tableName前缀
      if (isAgg || !relationalMeta.jdbcSupportGetTableNameFromResultSet()) {
        fullColumnNamesList.add(
            fullColumnNames.stream()
                .map(
                    s ->
                        RelationSchema.getQuoteFullName(tableName, s, quote)
                            + " AS "
                            + getQuotName(RelationSchema.getFullName(tableName, s)))
                .collect(Collectors.toList()));
      } else {
        fullColumnNamesList.add(
            fullColumnNames.stream()
                .map(s -> RelationSchema.getQuoteFullName(tableName, s, quote))
                .collect(Collectors.toList()));
      }
      fullColumnNamesListForExpandFilter.add(
          fullColumnNames.stream()
              .map(s -> RelationSchema.getFullName(tableName, s))
              .collect(Collectors.toList()));
      fullColumnNamesListForExpandFilter
          .get(fullColumnNamesListForExpandFilter.size() - 1)
          .add(RelationSchema.getFullName(tableName, KEY_NAME));
    }

    StringBuilder fullColumnNames = new StringBuilder();
    fullColumnNames.append(RelationSchema.getQuoteFullName(firstTable, KEY_NAME, quote));
    for (List<String> columnNames : fullColumnNamesList) {
      for (String columnName : columnNames) {
        fullColumnNames.append(", ").append(columnName);
      }
    }

    // 将Filter中的keyFilter替换成带tablename的value filter
    keyFilterAddTableName(filter, firstTable);

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
      filter = expandFilter(filter, fullColumnNamesListForExpandFilter);
      filter = LogicalFilterUtils.mergeTrue(filter);
    }

    String fullColumnNamesStr = fullColumnNames.toString();
    String filterStr = filterTransformer.toString(filter);
    String orderByKey = RelationSchema.getQuoteFullName(tableNames.get(0), KEY_NAME, quote);
    if (!relationalMeta.isSupportFullJoin()) {
      // 如果不支持full join,需要为left join + union模拟的full join表起别名，同时select、where、order by的部分都要调整
      fullColumnNamesStr = fullColumnNamesStr.replaceAll("`\\.`", ".");
      filterStr = filterStr.replaceAll("`\\.`", ".");
      filterStr =
          filterStr.replace(
              getQuotName(KEY_NAME), getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME));
      orderByKey = orderByKey.replaceAll("`\\.`", ".");
    }
    return String.format(
        relationalMeta.getQueryTableWithoutKeyStatement(),
        fullColumnNamesStr,
        fullTableName,
        filterStr.isEmpty() ? "" : "WHERE " + filterStr,
        orderByKey);
  }

  private String getProjectDummyWithSQL(
      Filter filter, String databaseName, Map<String, String> tableNameToColumnNames)
      throws SQLException {
    List<String> tableNames = new ArrayList<>();
    List<List<String>> fullColumnNamesList = new ArrayList<>();
    List<List<String>> fullQuoteColumnNamesList = new ArrayList<>();

    // 这里获取所有table的所有列名，用于concat时生成key列。
    Map<String, List<String>> allColumnNameForTable =
        getAllColumnNameForTable(databaseName, tableNameToColumnNames);

    // 将columnNames中的列名加上tableName前缀，带JOIN的查询语句中需要用到
    for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
      String tableName = entry.getKey();
      tableNames.add(tableName);
      List<String> columnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
      columnNames.replaceAll(s -> RelationSchema.getFullName(tableName, s));
      fullColumnNamesList.add(columnNames);

      List<String> fullColumnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
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
      copyFilter = expandFilter(copyFilter, fullColumnNamesList);
      copyFilter = LogicalFilterUtils.mergeTrue(copyFilter);
    }

    String filterStr = filterTransformer.toString(copyFilter);
    String orderByKey =
        buildConcat(
            fullQuoteColumnNamesList.stream().flatMap(List::stream).collect(Collectors.toList()));
    if (!relationalMeta.isSupportFullJoin()) {
      // 如果不支持full join,需要为left join + union模拟的full join表起别名，同时select、where、order by的部分都要调整
      char quote = relationalMeta.getQuote();
      fullQuoteColumnNamesList.forEach(
          columnNames -> columnNames.replaceAll(s -> s.replaceAll(quote + "\\." + quote, ".")));
      filterStr = filterStr.replaceAll("`\\.`", ".");
      filterStr =
          filterStr.replace(
              getQuotName(KEY_NAME), getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME));
      orderByKey = getQuotName(tableNames.get(0) + SEPARATOR + KEY_NAME);
    }

    return String.format(
        relationalMeta.getConcatQueryStatement(),
        buildConcat(
            fullQuoteColumnNamesList.stream().flatMap(List::stream).collect(Collectors.toList())),
        fullQuoteColumnNamesList.stream().flatMap(List::stream).collect(Collectors.joining(", ")),
        fullTableName,
        filterStr.isEmpty() ? "" : "WHERE " + filterStr,
        orderByKey);
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
      if (!relationalMeta.supportCreateDatabase()) {
        filter = reshapeFilterBeforeQuery(filter, databaseName);
      }
      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      Statement stmt;

      Map<String, String> tableNameToColumnNames =
          splitAndMergeQueryPatterns(databaseName, project.getPatterns());
      // 按列顺序加上表名
      Filter expandFilter =
          expandFilter(filter.copy(), tableNameToColumnNames, databaseName, false);

      String statement;
      // 如果table>1的情况下存在Value或Path Filter，说明filter的匹配需要跨table，此时需要将所有table join到一起进行查询
      if (FilterUtils.getAllPathsFromFilter(filter).stream().noneMatch(s -> s.contains("*"))
          && !(tableNameToColumnNames.size() > 1
              && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
        for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
          String tableName = entry.getKey();
          tableName = reshapeTableNameBeforeQuery(tableName, databaseName);
          String quotColumnNames = getQuotColumnNames(entry.getValue());
          if (!relationalMeta.jdbcSupportGetTableNameFromResultSet()) {
            quotColumnNames = getQuotTableAndColumnNames(tableName, entry.getValue());
          }

          String filterStr = filterTransformer.toString(expandFilter);
          statement =
              String.format(
                  relationalMeta.getQueryTableStatement(),
                  getQuotName(KEY_NAME),
                  quotColumnNames,
                  getQuotName(tableName),
                  filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                  getQuotName(KEY_NAME));

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
        statement =
            getProjectWithFilterSQL(databaseName, filter.copy(), tableNameToColumnNames, false);

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

      if (!relationalMeta.supportCreateDatabase()) {
        filter = reshapeFilterAfterQuery(filter, databaseName);
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
                  relationalMeta,
                  null,
                  null,
                  false));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute project task in %s failure", engineName), e));
    }
  }

  private String reshapeTableNameBeforeQuery(String tableName, String databaseName) {
    if (!relationalMeta.supportCreateDatabase()) {
      tableName = databaseName + "." + tableName;
    }
    return tableName;
  }

  private String reshapeTableNameAfterQuery(String tableName, String databaseName) {
    if (!relationalMeta.supportCreateDatabase()) {
      tableName = tableName.substring(databaseName.length() + 1);
    }
    return tableName;
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
      for (List<String> columnList : fullColumnList) {
        for (String column : columnList) {
          if (!column.contains(" AS ")) {
            column = String.format("%s AS %s", column, column.replaceAll("`\\.`", "."));
          }
          allColumns.append(column).append(", ");
        }
      }
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
      return dbStrategy.formatConcatStatement(concatList.get(0));
    }

    StringBuilder concat = new StringBuilder();
    concat.append(" CONCAT(");
    for (int i = 0; i < concatList.size(); i++) {
      concat.append(dbStrategy.formatConcatStatement(concatList.get(i)));
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

  private Filter expandFilter(
      Filter filter,
      Map<String, String> tableNameToColumnNames,
      String databaseName,
      boolean isDummy) {
    List<List<String>> fullColumnNamesList = new ArrayList<>();
    for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
      List<String> fullColumnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
      if (!relationalMeta.supportCreateDatabase() && !isDummy) {
        fullColumnNames.replaceAll(
            s ->
                RelationSchema.getQuoteFullName(
                    databaseName + SEPARATOR + entry.getKey(), s, relationalMeta.getQuote()));
      } else {
        // 将columnNames中的列名加上tableName前缀
        fullColumnNames.replaceAll(
            s -> RelationSchema.getQuoteFullName(entry.getKey(), s, relationalMeta.getQuote()));
      }
      fullColumnNamesList.add(fullColumnNames);
    }
    // 把fullColumnNamesList中的列名全部用removeFullColumnNameQuote去掉引号
    fullColumnNamesList.replaceAll(
        columnNames -> {
          List<String> newColumnNames = new ArrayList<>();
          for (String columnName : columnNames) {
            newColumnNames.add(removeFullColumnNameQuote(columnName));
          }
          return newColumnNames;
        });
    filter = expandFilter(filter, fullColumnNamesList);
    filter = LogicalFilterUtils.mergeTrue(filter);
    return filter;
  }

  private Filter reshapeFilterBeforeQuery(Filter filter, String databaseName) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = reshapeFilterBeforeQuery(child, databaseName);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = reshapeFilterBeforeQuery(child, databaseName);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = reshapeFilterBeforeQuery(notChild, databaseName);
        return new NotFilter(newFilter);
      case Value:
        ValueFilter valueFilter = ((ValueFilter) filter);
        String path = valueFilter.getPath();
        valueFilter.setPath(databaseName + SEPARATOR + path);
        return valueFilter;
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        inFilter.setPath(databaseName + SEPARATOR + inPath);
        return inFilter;
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        String pathA = pathFilter.getPathA();
        String pathB = pathFilter.getPathB();
        pathFilter.setPathA(databaseName + SEPARATOR + pathA);
        pathFilter.setPathB(databaseName + SEPARATOR + pathB);
        return pathFilter;
      default:
        break;
    }
    return filter;
  }

  private Filter reshapeFilterAfterQuery(Filter filter, String databaseName) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = reshapeFilterAfterQuery(child, databaseName);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = reshapeFilterAfterQuery(child, databaseName);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = reshapeFilterAfterQuery(notChild, databaseName);
        return new NotFilter(newFilter);
      case Value:
        ValueFilter valueFilter = ((ValueFilter) filter);
        String path = valueFilter.getPath();
        valueFilter.setPath(path.substring(path.indexOf(SEPARATOR) + 1));
        return valueFilter;
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        inFilter.setPath(inPath.substring(inPath.indexOf(SEPARATOR) + 1));
        return inFilter;
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        String pathA = pathFilter.getPathA();
        String pathB = pathFilter.getPathB();
        pathFilter.setPathA(pathA.substring(pathA.indexOf(SEPARATOR) + 1));
        pathFilter.setPathB(pathB.substring(pathB.indexOf(SEPARATOR) + 1));
        return pathFilter;
      default:
        break;
    }
    return filter;
  }

  private Filter expandFilter(Filter filter, List<List<String>> columnNamesList) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = expandFilter(child, columnNamesList);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = expandFilter(child, columnNamesList);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = expandFilter(notChild, columnNamesList);
        return new NotFilter(newFilter);
      case Value:
        ValueFilter valueFilter = ((ValueFilter) filter);
        String path = valueFilter.getPath();
        List<String> matchedPaths = getMatchedPath(path, columnNamesList);
        if (matchedPaths.isEmpty()) {
          return new BoolFilter(true);
        } else if (matchedPaths.size() == 1) {
          return new ValueFilter(matchedPaths.get(0), valueFilter.getOp(), valueFilter.getValue());
        } else {
          List<Filter> newFilters = new ArrayList<>();
          for (String matched : matchedPaths) {
            newFilters.add(new ValueFilter(matched, valueFilter.getOp(), valueFilter.getValue()));
          }
          if (Op.isOrOp(valueFilter.getOp())) {
            return new OrFilter(newFilters);
          } else {
            return new AndFilter(newFilters);
          }
        }
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        if (inPath.contains("*")) {
          List<String> matchedPath = getMatchedPath(inPath, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new InFilter(matchedPath.get(0), inFilter.getInOp(), inFilter.getValues());
          } else {
            List<Filter> inChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              inChildren.add(new InFilter(matched, inFilter.getInOp(), inFilter.getValues()));
            }

            if (inFilter.getInOp().isOrOp()) {
              return new OrFilter(inChildren);
            }
            return new AndFilter(inChildren);
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
            if (Op.isOrOp(((PathFilter) filter).getOp())) {
              filter = new OrFilter(andPathChildren);
            } else {
              filter = new AndFilter(andPathChildren);
            }
          }
        }

        if (pathB.contains("*")) {
          if (filter.getType() != FilterType.Path) {
            return expandFilter(filter, columnNamesList);
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
    for (List<String> columnNames : columnNamesList) {
      for (String columnName : columnNames) {
        Matcher matcher = pattern.matcher(splitFullName(columnName).k);
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
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        if (inPath.startsWith(databaseName + SEPARATOR)) {
          return new InFilter(
              inPath.substring(databaseName.length() + 1),
              inFilter.getInOp(),
              inFilter.getValues());
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

      List<ColumnField> columnFieldList = getColumns(databaseName, tableName, "%", true);
      for (ColumnField columnField : columnFieldList) {
        String columnName = columnField.getColumnName();

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

  @Override
  public boolean isSupportProjectWithAgg(Operator agg, DataArea dataArea, boolean isDummy) {
    if (agg.getType() != OperatorType.GroupBy && agg.getType() != OperatorType.SetTransform) {
      return false;
    }
    List<FunctionCall> functionCalls = OperatorUtils.getFunctionCallList(agg);
    for (FunctionCall functionCall : functionCalls) {
      if (!SUPPORTED_AGGREGATE_FUNCTIONS.contains(functionCall.getFunction().getIdentifier())) {
        return false;
      }
      if (functionCall.getParams().isDistinct()) return false;
    }

    if (isDummy) {
      List<String> patterns = new ArrayList<>();
      for (FunctionCall fc : functionCalls) {
        patterns.addAll(fc.getParams().getPaths());
      }
      patterns = patterns.stream().distinct().collect(Collectors.toList());
      try {
        Map<String, Map<String, String>> splitResults = splitAndMergeHistoryQueryPatterns(patterns);
        if (splitResults.size() != 1) {
          return false;
        }
      } catch (SQLException e) {
        return false;
      }
    }
    List<Expression> exprList = new ArrayList<>();
    functionCalls.forEach(fc -> exprList.addAll(fc.getParams().getExpressions()));
    // Group By Column和Function参数中不能带有函数，只能有四则运算表达式
    if (agg.getType() == OperatorType.GroupBy) {
      List<Expression> gbc = ((GroupBy) agg).getGroupByExpressions();
      exprList.addAll(gbc);
    }

    for (Expression expr : exprList) {
      final boolean[] isValid = {true};
      expr.accept(
          new ExpressionVisitor() {
            @Override
            public void visit(BaseExpression expression) {
              if (expression.getColumnName().contains("*")) {
                isValid[0] = false;
              }
            }

            @Override
            public void visit(BinaryExpression expression) {}

            @Override
            public void visit(BracketExpression expression) {}

            @Override
            public void visit(ConstantExpression expression) {}

            @Override
            public void visit(FromValueExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(FuncExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(MultipleExpression expression) {}

            @Override
            public void visit(UnaryExpression expression) {}

            @Override
            public void visit(CaseWhenExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(KeyExpression expression) {}

            @Override
            public void visit(SequenceExpression expression) {
              isValid[0] = false;
            }
          });

      if (!isValid[0]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean isSupportProjectWithAggSelect(
      Operator agg, Select select, DataArea dataArea, boolean isDummy) {
    return isSupportProjectWithAgg(agg, dataArea, isDummy) && isSupportProjectWithSelect();
  }

  @Override
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    List<FunctionCall> functionCalls = OperatorUtils.getFunctionCallList(agg);
    List<Expression> gbc = new ArrayList<>();
    if (agg.getType() == OperatorType.GroupBy) {
      gbc = ((GroupBy) agg).getGroupByExpressions();
    }
    try {
      String databaseName = dataArea.getStorageUnit();
      Connection conn = getConnection(databaseName);
      if (conn == null) {
        return new TaskExecuteResult(
            new RelationalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }
      Map<String, String> tableNameToColumnNames =
          splitAndMergeQueryPatterns(databaseName, project.getPatterns());

      String statement =
          getProjectWithFilterSQL(
              databaseName, select.getFilter().copy(), tableNameToColumnNames, true);
      if (statement.endsWith(";")) {
        statement = statement.substring(0, statement.length() - 1); // 去掉最后的分号
      }
      Map<String, String> fullName2Name = new HashMap<>();
      statement =
          generateAggSql(
              functionCalls,
              gbc,
              statement,
              tableNameToColumnNames,
              fullName2Name,
              databaseName,
              false);

      ResultSet rs = null;
      try {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(statement);
        LOGGER.info("[Query] execute query: {}", statement);
      } catch (SQLException e) {
        LOGGER.error("meet error when executing query {}: ", statement, e);
      }

      if (rs == null) {
        return new TaskExecuteResult(
            new RelationalTaskExecuteFailureException("execute query failure"));
      }

      Map<String, DataType> columnTypeMap = getSumDataType(functionCalls);

      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new RelationQueryRowStream(
                  Collections.singletonList(databaseName),
                  Collections.singletonList(rs),
                  false,
                  select.getFilter(),
                  project.getTagFilter(),
                  Collections.singletonList(conn),
                  relationalMeta,
                  columnTypeMap,
                  fullName2Name,
                  true));
      return new TaskExecuteResult(rowStream);

    } catch (SQLException | RelationalTaskExecuteFailureException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute project task in %s failure", engineName), e));
    }
  }

  private Map<String, DataType> getSumDataType(List<FunctionCall> functionCalls)
      throws RelationalTaskExecuteFailureException {
    // 如果下推的函数有sum,需要判断结果是小数还是整数
    List<Column> columns = null;
    Map<String, DataType> columnTypeMap = new HashMap<>();
    for (FunctionCall fc : functionCalls) {
      if (fc.getFunction().getIdentifier().equalsIgnoreCase(Sum.SUM)) {
        if (columns == null) {
          columns = getColumns(null, null).toList().blockingGet();
        }
        if (isSumResultDouble(fc.getParams().getExpression(0), columns)) {
          columnTypeMap.put(fc.getFunctionStr(), DataType.DOUBLE);
        } else {
          columnTypeMap.put(fc.getFunctionStr(), DataType.LONG);
        }
      }
    }
    return columnTypeMap;
  }

  private String generateAggSql(
      List<FunctionCall> functionCalls,
      List<Expression> gbc,
      String statement,
      Map<String, String> table2Column,
      Map<String, String> fullName2Name,
      String databaseName,
      boolean isDummy) {
    char quote = relationalMeta.getQuote();
    List<String> fullColumnNames = new ArrayList<>();
    for (Map.Entry<String, String> entry : table2Column.entrySet()) {
      String tableName = entry.getKey();
      List<String> columnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
      for (String columnName : columnNames) {
        fullColumnNames.add(tableName + SEPARATOR + columnName);
      }
    }
    // 在statement的基础上添加group by和函数内容
    // 这里的处理形式是生成一个形如
    // SELECT max(derived."a") AS "max(test.a)", sum(derived."b") AS "sum(test.b)", derived."c" AS
    // "test.c"
    // FROM (SELECT a, b, c, d FROM test) AS derived GROUP BY c;
    // 的SQL语句
    // 几个注意的点，1. 嵌套子查询必须重命名，否则会报错，这里重命名为derived。
    // 2. 查询出来的结果也需要重命名来适配原始的列名，这里重命名为"max(test.a)"
    StringBuilder sqlColumnsStr = new StringBuilder();
    for (FunctionCall functionCall : functionCalls) {
      String functionName = functionCall.getFunction().getIdentifier();
      Expression param = functionCall.getParams().getExpressions().get(0);

      List<Expression> expandExprs = expandExpression(param, fullColumnNames);
      for (Expression expr : expandExprs) {
        String IGinXTagKVName =
            String.format(
                "%s(%s)", functionName, exprToIGinX(ExprUtils.copy(expr)).getColumnName());
        fullName2Name.put(IGinXTagKVName, functionCall.getFunctionStr());

        String format = "%s(%s)";
        // 如果是avg函数，且参数是base类型，在mysql下小数位数仅有5位，需要转换为decimal来补齐
        // 仅在mysql下这么做，pg也可以用，但会出现一些误差，例如3.200000和3.1999999的区别，测试不好通过
        if (functionName.equalsIgnoreCase(Avg.AVG)) {
          format = dbStrategy.getAvgCastExpression(param);
        }
        expr = reshapeExpressionColumnNameBeforeQuery(ExprUtils.copy(expr), databaseName, isDummy);
        sqlColumnsStr.append(
            String.format(
                format, functionName, getJdbcExpressionString(exprAdapt(ExprUtils.copy(expr)))));
        sqlColumnsStr.append(" AS ");
        sqlColumnsStr.append(quote).append(IGinXTagKVName).append(quote);
        sqlColumnsStr.append(", ");
      }
    }

    for (int i = 0; i < gbc.size(); i++) {
      Expression expr = gbc.get(i);
      String originColumnStr = quote + expr.getColumnName() + quote;
      expr =
          reshapeExpressionColumnNameBeforeQuery(ExprUtils.copy(gbc.get(i)), databaseName, isDummy);
      gbc.set(i, expr);
      sqlColumnsStr
          .append(getJdbcExpressionString(exprAdapt(ExprUtils.copy(expr))))
          .append(" AS ")
          .append(originColumnStr)
          .append(", ");
    }
    sqlColumnsStr.delete(sqlColumnsStr.length() - 2, sqlColumnsStr.length());

    statement = "SELECT " + sqlColumnsStr + " FROM (" + statement + ") derived";
    if (!gbc.isEmpty()) {
      statement +=
          " GROUP BY "
              + gbc.stream()
                  .map(e -> getJdbcExpressionString(exprAdapt(ExprUtils.copy(e))))
                  .collect(Collectors.joining(", "));
    }
    gbc.replaceAll(
        expression ->
            reshapeExpressionColumnNameAfterQuery(
                ExprUtils.copy(expression), databaseName, isDummy));
    return statement;
  }

  private String getJdbcExpressionString(Expression expr) {
    if (expr instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expr;
      cn.edu.tsinghua.iginx.engine.shared.expr.Operator op = binaryExpr.getOp();
      if (op == cn.edu.tsinghua.iginx.engine.shared.expr.Operator.MOD) {
        return String.format(
            "MOD(%s, %s)",
            getJdbcExpressionString(binaryExpr.getLeftExpression()),
            getJdbcExpressionString(binaryExpr.getRightExpression()));
      }
    }
    return expr.getCalColumnName();
  }

  private Expression reshapeExpressionColumnNameBeforeQuery(
      Expression expr, String databaseName, boolean isDummy) {
    Expression.ExpressionType expressionType = expr.getType();
    switch (expressionType) {
      case Base:
        // 不支持创建数据库的情况下，数据库名作为tableName的一部分
        BaseExpression baseExpr = (BaseExpression) expr;
        if (!relationalMeta.supportCreateDatabase() && !isDummy) {
          baseExpr.setPathName(databaseName + SEPARATOR + expr.getColumnName());
        }
        return baseExpr;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expr;
        binaryExpression.setLeftExpression(
            reshapeExpressionColumnNameBeforeQuery(
                binaryExpression.getLeftExpression(), databaseName, isDummy));
        binaryExpression.setRightExpression(
            reshapeExpressionColumnNameBeforeQuery(
                binaryExpression.getRightExpression(), databaseName, isDummy));
        return binaryExpression;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expr;
        bracketExpression.setExpression(
            reshapeExpressionColumnNameBeforeQuery(
                bracketExpression.getExpression(), databaseName, isDummy));
        return bracketExpression;
      case Function:
        FuncExpression funcExpression = (FuncExpression) expr;
        funcExpression
            .getExpressions()
            .replaceAll(
                expression ->
                    reshapeExpressionColumnNameBeforeQuery(expression, databaseName, isDummy));
        return funcExpression;
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expr;
        multipleExpression
            .getChildren()
            .replaceAll(
                expression ->
                    reshapeExpressionColumnNameBeforeQuery(expression, databaseName, isDummy));
        return multipleExpression;
      default:
        break;
    }
    return expr;
  }

  private Expression reshapeExpressionColumnNameAfterQuery(
      Expression expr, String databaseName, boolean isDummy) {
    Expression.ExpressionType expressionType = expr.getType();
    switch (expressionType) {
      case Base:
        // 不支持创建数据库的情况下，数据库名作为tableName的一部分
        BaseExpression baseExpr = (BaseExpression) expr;
        if (!relationalMeta.supportCreateDatabase() && !isDummy) {
          baseExpr.setPathName(expr.getColumnName().substring(databaseName.length() + 1));
        }
        return baseExpr;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expr;
        binaryExpression.setLeftExpression(
            reshapeExpressionColumnNameAfterQuery(
                binaryExpression.getLeftExpression(), databaseName, isDummy));
        binaryExpression.setRightExpression(
            reshapeExpressionColumnNameAfterQuery(
                binaryExpression.getRightExpression(), databaseName, isDummy));
        return binaryExpression;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expr;
        bracketExpression.setExpression(
            reshapeExpressionColumnNameAfterQuery(
                bracketExpression.getExpression(), databaseName, isDummy));
        return bracketExpression;
      case Function:
        FuncExpression funcExpression = (FuncExpression) expr;
        funcExpression
            .getExpressions()
            .replaceAll(
                expression ->
                    reshapeExpressionColumnNameAfterQuery(expression, databaseName, isDummy));
        return funcExpression;
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expr;
        multipleExpression
            .getChildren()
            .replaceAll(
                expression ->
                    reshapeExpressionColumnNameAfterQuery(expression, databaseName, isDummy));
        return multipleExpression;
      default:
        break;
    }
    return expr;
  }

  /**
   * 表达式适配下推到PG的形式 1.将baseExpression转换为QuoteBaseExpression，以让其在SQL中被引号包裹
   * 如果SQL使用了JOIN,那列名形如`table.column`，如果没有，则形如`table`.`column`
   */
  private Expression exprAdapt(Expression expr) {
    if (expr instanceof BaseExpression) {
      return new QuoteBaseExpressionDecorator((BaseExpression) expr, relationalMeta.getQuote());
    }
    expr.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {}

          @Override
          public void visit(BinaryExpression expression) {
            if (expression.getLeftExpression() instanceof BaseExpression) {
              expression.setLeftExpression(
                  new QuoteBaseExpressionDecorator(
                      (BaseExpression) expression.getLeftExpression(), relationalMeta.getQuote()));
            }
            if (expression.getRightExpression() instanceof BaseExpression) {
              expression.setRightExpression(
                  new QuoteBaseExpressionDecorator(
                      (BaseExpression) expression.getRightExpression(), relationalMeta.getQuote()));
            }
          }

          @Override
          public void visit(BracketExpression expression) {
            if (expression.getExpression() instanceof BaseExpression) {
              expression.setExpression(
                  new QuoteBaseExpressionDecorator(
                      (BaseExpression) expression.getExpression(), relationalMeta.getQuote()));
            }
          }

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {
            for (int i = 0; i < expression.getExpressions().size(); i++) {
              if (expression.getExpressions().get(i) instanceof BaseExpression) {
                expression
                    .getExpressions()
                    .set(
                        i,
                        new QuoteBaseExpressionDecorator(
                            (BaseExpression) expression.getExpressions().get(i),
                            relationalMeta.getQuote()));
              }
            }
          }

          @Override
          public void visit(MultipleExpression expression) {
            for (int i = 0; i < expression.getChildren().size(); i++) {
              if (expression.getChildren().get(i) instanceof BaseExpression) {
                expression
                    .getChildren()
                    .set(
                        i,
                        new QuoteBaseExpressionDecorator(
                            (BaseExpression) expression.getChildren().get(i),
                            relationalMeta.getQuote()));
              }
            }
          }

          @Override
          public void visit(UnaryExpression expression) {
            if (expression.getExpression() instanceof BaseExpression) {
              expression.setExpression(
                  new QuoteBaseExpressionDecorator(
                      (BaseExpression) expression.getExpression(), relationalMeta.getQuote()));
            }
          }

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });

    return expr;
  }

  /** 表达式修改成取回到IGinX的形式，TagKV的形式要是{t=v1, t2=v2} */
  private Expression exprToIGinX(Expression expr) {
    expr.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            String fullColumnName = expression.getColumnName();
            Pair<String, Map<String, String>> split = splitFullName(fullColumnName);
            if (split.v == null || split.v.isEmpty()) {
              return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(split.k);
            sb.append("{");
            Map<String, String> tagKV = split.v;
            for (Map.Entry<String, String> entry : tagKV.entrySet()) {
              sb.append(entry.getKey());
              sb.append("=");
              sb.append(entry.getValue());
              sb.append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append("}");
            expression.setPathName(sb.toString());
          }

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {}

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });
    return expr;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    List<Connection> connList = new ArrayList<>();
    Filter filter = select.getFilter();
    List<FunctionCall> functionCalls = OperatorUtils.getFunctionCallList(agg);
    List<Expression> gbc = new ArrayList<>();
    if (agg.getType() == OperatorType.GroupBy) {
      gbc = ((GroupBy) agg).getGroupByExpressions();
    }
    try {
      List<String> databaseNameList = new ArrayList<>();
      List<ResultSet> resultSets = new ArrayList<>();
      ResultSet rs = null;
      Connection conn;
      Statement stmt;
      String statement;

      // 如果下推的函数有sum,需要判断结果是小数还是整数
      Map<String, DataType> columnTypeMap = getSumDataType(functionCalls);

      Map<String, Map<String, String>> splitResults =
          splitAndMergeHistoryQueryPatterns(project.getPatterns());
      Map<String, String> fullName2Name = new HashMap<>();
      for (Map.Entry<String, Map<String, String>> splitEntry : splitResults.entrySet()) {
        Map<String, String> tableNameToColumnNames = splitEntry.getValue();
        String databaseName = splitEntry.getKey();
        conn = getConnection(databaseName);
        if (conn == null) {
          continue;
        }
        connList.add(conn);

        // 如果table没有带通配符，那直接简单构建起查询语句即可
        statement = getProjectDummyWithSQL(filter, databaseName, tableNameToColumnNames);
        gbc.forEach(this::cutExprDatabaseNameForDummy);
        functionCalls.forEach(
            fc -> fc.getParams().getExpressions().forEach(this::cutExprDatabaseNameForDummy));

        statement =
            generateAggSql(
                functionCalls,
                gbc,
                statement,
                tableNameToColumnNames,
                fullName2Name,
                databaseName,
                true);

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
                  true,
                  filter,
                  project.getTagFilter(),
                  connList,
                  relationalMeta,
                  columnTypeMap,
                  fullName2Name,
                  true));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException | RelationalTaskExecuteFailureException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new RelationalTaskExecuteFailureException(
              String.format("execute project task in %s failure", engineName), e));
    }
  }

  @Override
  public TaskExecuteResult executeProjectWithAgg(Project project, Operator agg, DataArea dataArea) {
    return executeProjectWithAggSelect(
        project,
        new Select(new OperatorSource(project), new BoolFilter(true), null),
        agg,
        dataArea);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAgg(
      Project project, Operator agg, DataArea dataArea) {
    return executeProjectDummyWithAggSelect(
        project,
        new Select(new OperatorSource(project), new BoolFilter(true), null),
        agg,
        dataArea);
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

        // 如果table没有带通配符，那直接简单构建起查询语句即可
        if (!filter.toString().contains("*")
            && !(tableNameToColumnNames.size() > 1
                && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
          Filter expandFilter =
              expandFilter(
                  cutFilterDatabaseNameForDummy(filter.copy(), databaseName),
                  tableNameToColumnNames,
                  databaseName,
                  true);
          for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
            String tableName = entry.getKey();
            String fullQuotColumnNames = getQuotColumnNames(entry.getValue());
            if (!relationalMeta.jdbcSupportGetTableNameFromResultSet()) {
              fullQuotColumnNames = getQuotTableAndColumnNames(tableName, entry.getValue());
            }
            List<String> fullPathList = Arrays.asList(entry.getValue().split(", "));
            fullPathList.replaceAll(s -> RelationSchema.getFullName(tableName, s));
            List<String> fullQuotePathList = Arrays.asList(entry.getValue().split(", "));
            fullQuotePathList.replaceAll(
                s -> RelationSchema.getQuoteFullName(tableName, s, relationalMeta.getQuote()));

            String filterStr =
                filterTransformer.toString(
                    dummyFilterSetTrueByColumnNames(expandFilter.copy(), fullPathList));
            String concatKey = buildConcat(fullQuotePathList);
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
          statement = getProjectDummyWithSQL(filter, databaseName, tableNameToColumnNames);

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
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        if (!inPath.contains("*") && !columnNameList.contains(inPath)) {
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
          if (relationalMeta.supportCreateDatabase()) {
            // 删除整个数据库
            closeConnection(databaseName);
            Connection defaultConn =
                getConnection(
                    relationalMeta.getDefaultDatabaseName()); // 正在使用的数据库无法被删除，因此需要切换到默认数据库
            if (defaultConn != null) {
              try {
                stmt = defaultConn.createStatement();
                statement =
                    String.format(
                        relationalMeta.getDropDatabaseStatement(), getQuotName(databaseName));
                LOGGER.info("[Delete] execute delete: {}", statement);
                stmt.execute(statement); // 删除数据库
              } catch (Exception ignoreExcetpion) {
                // add try-catch for dbs which do not support "DROP DATABASE IF EXISTS %s" grammar,
                // e.g.dameng
              } finally {
                stmt.close();
                defaultConn.close();
                conn.close();
              }
              return new TaskExecuteResult(null, null);
            } else {
              conn.close();
              return new TaskExecuteResult(
                  new RelationalTaskExecuteFailureException(
                      String.format(
                          "cannot connect to database %s", relationalMeta.getDefaultDatabaseName()),
                      new SQLException()));
            }
          } else {
            tables = getTables(databaseName, "%", false);
            if (!tables.isEmpty()) {
              String statementTemplate = relationalMeta.getDropTableStatement();
              List<String> statements =
                  tables.stream()
                      .map(table -> reshapeTableNameBeforeQuery(table, databaseName))
                      .map(table -> String.format(statementTemplate, getQuotName(table)))
                      .collect(Collectors.toList());
              LOGGER.info("[Delete] execute delete: {}", statements);
              for (String dropTableStatement : statements) {
                stmt.addBatch(dropTableStatement);
              }
              stmt.executeBatch();
            }
          }
        } else {
          deletedPaths = determineDeletedPaths(paths, delete.getTagFilter());
          for (Pair<String, String> pair : deletedPaths) {
            tableName = pair.k;
            columnName = pair.v;
            tables = getTables(databaseName, tableName, false);
            tableName = reshapeTableNameBeforeQuery(tableName, databaseName);
            if (!tables.isEmpty()) {
              statement =
                  String.format(
                      relationalMeta.getAlterTableDropColumnStatement(),
                      getQuotName(tableName),
                      getQuotName(columnName));
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
          if (!getColumns(databaseName, tableName, columnName, false).isEmpty()) {
            tableName = reshapeTableNameBeforeQuery(tableName, databaseName);
            for (KeyRange keyRange : delete.getKeyRanges()) {
              statement =
                  String.format(
                      relationalMeta.getDeleteTableStatement(),
                      getQuotName(tableName),
                      getQuotName(columnName),
                      getQuotName(KEY_NAME),
                      keyRange.getBeginKey(),
                      getQuotName(KEY_NAME),
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
      for (String databaseName : getDatabaseNames(true)) {
        List<String> tables = getTables(databaseName, "%", true);
        for (String tableName : tables) {
          List<ColumnField> columnFieldList = getColumns(databaseName, tableName, "%", true);
          for (ColumnField columnField : columnFieldList) {
            String columnName = columnField.getColumnName(); // 获取列名称

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
      String databaseName, String tableName, String columnNames, boolean isDummy) {
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
    for (String pattern : patterns) {
      String tableName;
      String columnNames;
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
            getColumns(
                databaseName, reformatForJDBC(tableName), reformatForJDBC(columnNames), false);
      } else {
        columnFieldList = getColumns(databaseName, "%", "%", false);
      }

      List<Pattern> patternList =
          getRegexPatternByName(databaseName, tableName, columnNames, false);
      Pattern tableNamePattern = patternList.get(0), columnNamePattern = patternList.get(1);

      for (ColumnField columnField : columnFieldList) {
        String curTableName = columnField.getTableName();
        String curColumnNames = columnField.getColumnName();
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
    List<String> databases = getDatabaseNames(true);
    Map<String, Map<String, String>> splitResults = new HashMap<>();

    // Process patterns in parallel using ExecutorCompletionService
    List<Future<Map<String, Map<String, String>>>> futures = new ArrayList<>();

    // Submit tasks
    for (String pattern : patterns) {
      futures.add(executor.submit(() -> splitAndMergeHistoryQueryPattern(pattern, databases)));
    }
    // Collect results from all futures
    List<ListenableFuture<Map<String, Map<String, String>>>> listenableFutures =
        futures.stream().map(JdkFutureAdapters::listenInPoolThread).collect(Collectors.toList());
    List<Map<String, Map<String, String>>> results;
    try {
      results = Futures.allAsList(listenableFutures).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException(e);
    }

    for (Map<String, Map<String, String>> result : results) {
      for (Map.Entry<String, Map<String, String>> entry : result.entrySet()) {
        String databaseName = entry.getKey();
        Map<String, String> tableNameToColumnNames = entry.getValue();

        // Get or create the map for this database
        Map<String, String> existingDbMap =
            splitResults.computeIfAbsent(databaseName, k -> new HashMap<>());

        // Merge the table/column mappings
        for (Map.Entry<String, String> tableEntry : tableNameToColumnNames.entrySet()) {
          String tableName = tableEntry.getKey();
          String newColumns = tableEntry.getValue();

          existingDbMap.merge(
              tableName, newColumns, (oldColumns, columns) -> oldColumns + ", " + columns);
        }
      }
    }

    return splitResults;
  }

  private Map<String, Map<String, String>> splitAndMergeHistoryQueryPattern(
      String pattern, List<String> databases) throws SQLException {
    Map<String, Map<String, String>> splitResults = new HashMap<>();
    String tableName;
    String databaseName;
    String columnNames;
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

    List<Pattern> patternList = getRegexPatternByName(databaseName, tableName, columnNames, true);
    Pattern tableNamePattern = patternList.get(0), columnNamePattern = patternList.get(1);

    if (databaseName.equals("%")) {
      for (String tempDatabaseName : getDatabaseNames(true)) {
        if (tempDatabaseName.startsWith(DATABASE_PREFIX)) {
          continue;
        }
        List<String> tables = getTables(tempDatabaseName, tableName, true);
        for (String tempTableName : tables) {
          if (!tableNamePattern.matcher(tempTableName).find()) {
            continue;
          }
          List<ColumnField> columnFieldList =
              getColumns(tempDatabaseName, tempTableName, columnNames, true);
          for (ColumnField columnField : columnFieldList) {
            String tempColumnNames = columnField.getColumnName();
            if (!columnNamePattern.matcher(tempColumnNames).find()) {
              continue;
            }
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
      if (!databases.contains(databaseName)) {
        return splitResults;
      }
      List<ColumnField> columnFieldList = getColumns(databaseName, tableName, columnNames, true);
      Map<String, String> tableNameToColumnNames = new HashMap<>();
      for (ColumnField columnField : columnFieldList) {
        tableName = columnField.getTableName();
        columnNames = columnField.getColumnName();
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
            String[] oldColumnNameList = (oldColumnNames + ", " + entry.getValue()).split(", ");
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

        List<String> tables = getTables(storageUnit, tableName, false);
        columnName = toFullName(columnName, tags);
        if (tables.isEmpty()) {
          tableName = reshapeTableNameBeforeQuery(tableName, storageUnit);
          String statement =
              String.format(
                  relationalMeta.getCreateTableStatement(),
                  getQuotName(tableName),
                  getQuotName(KEY_NAME),
                  relationalMeta.getDataTypeTransformer().toEngineType(DataType.LONG),
                  getQuotName(columnName),
                  relationalMeta.getDataTypeTransformer().toEngineType(dataType),
                  getQuotName(KEY_NAME));
          LOGGER.info("[Create] execute create: {}", statement);
          stmt.execute(statement);
        } else {
          if (getColumns(storageUnit, tableName, columnName, false).isEmpty()) {
            tableName = reshapeTableNameBeforeQuery(tableName, storageUnit);
            String statement =
                String.format(
                    relationalMeta.getAlterTableAddColumnStatement(),
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

        executeBatchInsert(conn, databaseName, stmt, tableToColumnEntries);
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
        executeBatchInsert(conn, databaseName, stmt, tableToColumnEntries);
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
      Connection conn,
      String databaseName,
      Statement stmt,
      Map<String, Pair<String, List<String>>> tableToColumnEntries)
      throws SQLException {
    dbStrategy.executeBatchInsert(
        conn, databaseName, stmt, tableToColumnEntries, relationalMeta.getQuote());
  }

  private List<Pair<String, String>> determineDeletedPaths(
      List<String> paths, TagFilter tagFilter) {
    try {
      List<Column> columns = getColumns(null, null).toList().blockingGet();
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

  private String getQuotTableAndColumnNames(String tableName, String columnNames) {
    String[] parts = columnNames.split(", ");
    StringBuilder fullColumnNames = new StringBuilder();
    for (String part : parts) {
      fullColumnNames.append(
          RelationSchema.getQuoteFullName(tableName, part, relationalMeta.getQuote()));
      fullColumnNames.append(" AS ");
      fullColumnNames.append(getQuotName(RelationSchema.getFullName(tableName, part)));
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

  private boolean isSumResultDouble(Expression expr, List<Column> columns) {
    Map<String, DataType> columnTypeMap = new HashMap<>();
    for (Column column : columns) {
      columnTypeMap.put(splitFullName(column.getPath()).k, column.getDataType());
    }
    boolean[] isDouble = {false};
    expr.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            String path = expression.getColumnName();
            if (!relationalMeta.supportCreateDatabase() && path.startsWith(DATABASE_PREFIX)) {
              int firstSeparatorIndex = path.indexOf(SEPARATOR);
              path = path.substring(firstSeparatorIndex + 1);
            }
            if (columnTypeMap.containsKey(path)) {
              isDouble[0] |= columnTypeMap.get(path) == DataType.DOUBLE;
            }
          }

          @Override
          public void visit(BinaryExpression expression) {
            isDouble[0] |=
                expression.getOp() == cn.edu.tsinghua.iginx.engine.shared.expr.Operator.DIV;
          }

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {
            isDouble[0] |=
                expression.getValue() instanceof Double || expression.getValue() instanceof Float;
          }

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {}

          @Override
          public void visit(MultipleExpression expression) {
            isDouble[0] |=
                expression.getOps().stream()
                    .anyMatch(op -> op == cn.edu.tsinghua.iginx.engine.shared.expr.Operator.DIV);
          }

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });

    return isDouble[0];
  }

  private void cutExprDatabaseNameForDummy(Expression expr) {
    expr.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            expression.setPathName(expression.getColumnName().split("\\.")[1]);
          }

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {}

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });
  }

  /**
   * 获取替换了*和tag kv的expression
   *
   * @return
   */
  private List<Expression> expandExpression(Expression expression, List<String> fullColumnNames) {
    Queue<Expression> queue = new LinkedList<>();
    List<Expression> result = new ArrayList<>();
    queue.add(expression);
    while (!queue.isEmpty()) {
      Expression expr = queue.poll();
      BaseExpression[] be = {null};
      List<String> matchesColumns = new ArrayList<>();
      expr.accept(
          new ExpressionVisitor() {
            @Override
            public void visit(BaseExpression expression) {
              if (be[0] != null) return;
              if (expression.getColumnName().contains("*")) {
                be[0] = expression;
              }
              String pattern = StringUtils.reformatPath(expression.getColumnName());
              for (String fullColumnName : fullColumnNames) {
                String columnNameWithoutTag = splitFullName(fullColumnName).k;
                if (Pattern.matches(pattern, columnNameWithoutTag)) {
                  matchesColumns.add(fullColumnName);
                }
              }
              if (matchesColumns.size() > 1) {
                be[0] = expression;
              } else {
                matchesColumns.clear();
              }
            }

            @Override
            public void visit(BinaryExpression expression) {}

            @Override
            public void visit(BracketExpression expression) {}

            @Override
            public void visit(ConstantExpression expression) {}

            @Override
            public void visit(FromValueExpression expression) {}

            @Override
            public void visit(FuncExpression expression) {}

            @Override
            public void visit(MultipleExpression expression) {}

            @Override
            public void visit(UnaryExpression expression) {}

            @Override
            public void visit(CaseWhenExpression expression) {}

            @Override
            public void visit(KeyExpression expression) {}

            @Override
            public void visit(SequenceExpression expression) {}
          });

      if (be[0] == null) {
        result.add(expr);
        continue;
      }
      for (String columnName : matchesColumns) {
        be[0].setPathName(columnName);
        queue.add(ExprUtils.copy(expr));
      }
    }

    return result;
  }

  private void keyFilterAddTableName(Filter filter, String tableName) {
    switch (filter.getType()) {
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        orChildren.replaceAll(child -> keyFilter2ValueFilter(child, tableName));
        orChildren.forEach(child -> keyFilterAddTableName(child, tableName));
        break;
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        andChildren.replaceAll(child -> keyFilter2ValueFilter(child, tableName));
        andChildren.forEach(child -> keyFilterAddTableName(child, tableName));
        break;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        notFilter.setChild(keyFilter2ValueFilter(notFilter.getChild(), tableName));
        keyFilterAddTableName(notFilter.getChild(), tableName);
        break;
    }
  }

  private Filter keyFilter2ValueFilter(Filter filter, String tableName) {
    if (filter.getType() != FilterType.Key) {
      return filter;
    }
    KeyFilter keyFilter = (KeyFilter) filter;
    return new ValueFilter(
        tableName + SEPARATOR + KEY_NAME, keyFilter.getOp(), new Value(keyFilter.getValue()));
  }
}
