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
package cn.edu.tsinghua.iginx.relational;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
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
import cn.edu.tsinghua.iginx.relational.query.entity.RelationQueryRowStream;
import cn.edu.tsinghua.iginx.relational.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.tools.RelationSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RelationAbstractStorage implements IStorage {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RelationAbstractStorage.class);

  protected final StorageEngineMeta meta;

  protected final Connection connection;

  /**
   * 获取ENGINE的默认用户名
   *
   * @return ENGINE的默认用户名
   */
  protected abstract String getDefaultUsername();

  /**
   * 获取ENGINE的默认密码
   *
   * @return ENGINE的默认密码
   */
  protected abstract String getDefaultPassword();

  /**
   * 获取ENGINE的默认数据库名称
   *
   * @return ENGINE的默认数据库名称
   */
  protected abstract String getDefaultDatabaseName();

  /**
   * 获取ENGINE的名称
   *
   * @return ENGINE的名称
   */
  protected abstract String getEngineName();

  /**
   * 获取ENGINE的驱动类
   *
   * @return ENGINE的驱动类
   */
  protected abstract String getDriverClass();

  /** 对ENGINE设置连接超时 */
  protected abstract void setConnectionTimeout(Statement stmt) throws SQLException;

  /**
   * 获取ENGINE的数据类型转换器
   *
   * @return ENGINE的数据类型转换器
   */
  protected abstract IDataTypeTransformer getDataTypeTransformer();

  /**
   * 该函数要求子类维护一个数据库连接池，根据数据库名称获取一个数据库连接
   *
   * @param databaseName 数据库名称
   * @return 数据库连接
   */
  protected abstract Connection getConnectionFromPool(String databaseName);

  /**
   * 使用JDBC获取该ENGINE的所有数据库名称
   *
   * @return 数据库名称列表
   */
  protected abstract List<String> getDatabaseNames();

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

    }

    return getConnectionFromPool(databaseName);
  }

  public RelationAbstractStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, getDefaultUsername());
    String password = extraParams.getOrDefault(PASSWORD, getDefaultPassword());
    String connUrl =
        String.format(
            "jdbc:%s://%s:%s/?user=%s&password=%s",
            getEngineName(), meta.getIp(), meta.getPort(), username, password);
    try {
      connection = DriverManager.getConnection(connUrl);
      Statement statement = connection.createStatement();
      setConnectionTimeout(statement);
      statement.close();
    } catch (SQLException e) {
      throw new StorageInitializationException("cannot connect to " + meta);
    }
  }

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, getDefaultUsername());
    String password = extraParams.getOrDefault(PASSWORD, getDefaultPassword());
    String connUrl =
        String.format(
            "jdbc:%s://%s:%s/?user=%s&password=%s",
            getEngineName(), meta.getIp(), meta.getPort(), username, password);
    try {
      Class.forName(getDriverClass());
      DriverManager.getConnection(connUrl);
      return true;
    } catch (SQLException | ClassNotFoundException e) {
      return false;
    }
  }

  protected String getUrl(String databaseName) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, getDefaultUsername());
    String password = extraParams.getOrDefault(PASSWORD, getDefaultPassword());
    return String.format(
        "jdbc:%s://%s:%s/%s?user=%s&password=%s",
        getEngineName(), meta.getIp(), meta.getPort(), databaseName, username, password);
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
      for (String databaseName : getDatabaseNames()) {
        if ((extraParams.get("has_data") == null || extraParams.get("has_data").equals("false"))
            && !databaseName.startsWith(DATABASE_PREFIX)) {
          continue;
        }

        try {
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
                        getDataTypeTransformer().fromEngineType(typeName),
                        nameAndTags.v));
              } else {
                columns.add(
                    new Column(
                        databaseName + SEPARATOR + tableName + SEPARATOR + nameAndTags.k,
                        getDataTypeTransformer().fromEngineType(typeName),
                        nameAndTags.v));
              }
            }
            columnSet.close();
          }
          tableSet.close();
          conn.close();
        } catch (SQLException e) {
          LOGGER.error("unexpected error: ", e);
        }
      }
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
          String filterStr = FilterTransformer.toString(filter);
          statement =
              String.format(
                  QUERY_STATEMENT,
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
          fullColumnNames.replaceAll(s -> RelationSchema.getQuotFullName(tableName, s));
          fullColumnNamesList.add(fullColumnNames);
        }

        StringBuilder fullColumnNames = new StringBuilder();
        fullColumnNames.append(RelationSchema.getQuotFullName(tableNames.get(0), KEY_NAME));
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
                .append(RelationSchema.getQuotFullName(tableNames.get(i), KEY_NAME))
                .append(" = ")
                .append(RelationSchema.getQuotFullName(tableNames.get(j), KEY_NAME));

            if (j != i - 1) {
              fullTableName.append(" AND ");
            }
          }
          fullTableName.append(")");
        }

        // 对通配符做处理，将通配符替换成对应的列名
        if (FilterTransformer.toString(filter).contains("*")) {
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

        String filterStr = FilterTransformer.toString(filter);
        statement =
            String.format(
                QUERY_STATEMENT_WITHOUT_KEYNAME,
                fullColumnNames,
                fullTableName,
                filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                RelationSchema.getQuotFullName(tableNames.get(0), KEY_NAME));

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
                  getDataTypeTransformer()));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute project task in %s failure", getEngineName()), e));
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
    return fullColumnName.substring(1, fullColumnName.length() - 1).replace("\".\"", ".");
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
        columnNames = RelationSchema.getQuotFullName(tableName, rs.getString("COLUMN_NAME"));
        if (allColumnNameForTable.containsKey(tableName)) {
          columnNames = allColumnNameForTable.get(tableName) + ", " + columnNames;
        }
        allColumnNameForTable.put(tableName, columnNames);
      }
      conn.close();
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

        if (!filter.toString().contains("*")
            && !(tableNameToColumnNames.size() > 1
                && filterContainsType(Arrays.asList(FilterType.Value, FilterType.Path), filter))) {
          Map<String, String> allColumnNameForTable =
              getAllColumnNameForTable(databaseName, tableNameToColumnNames);
          for (Map.Entry<String, String> entry : splitEntry.getValue().entrySet()) {
            String tableName = entry.getKey();
            String fullQuotColumnNames = getQuotColumnNames(entry.getValue());
            List<String> fullPathList = Arrays.asList(entry.getValue().split(", "));
            fullPathList.replaceAll(s -> RelationSchema.getQuotFullName(tableName, s));
            String filterStr =
                FilterTransformer.toString(
                    dummyFilterSetTrueByColumnNames(
                        cutFilterDatabaseNameForDummy(filter.copy(), databaseName), fullPathList));
            statement =
                String.format(
                    CONCAT_QUERY_STATEMENT_AND_CONCAT_KEY,
                    allColumnNameForTable.get(tableName),
                    fullQuotColumnNames,
                    getQuotName(tableName),
                    filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                    allColumnNameForTable.get(tableName));

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
          List<List<String>> fullQuotColumnNamesList = new ArrayList<>();

          // 将columnNames中的列名加上tableName前缀，带JOIN的查询语句中需要用到
          for (Map.Entry<String, String> entry : tableNameToColumnNames.entrySet()) {
            String tableName = entry.getKey();
            tableNames.add(tableName);
            List<String> columnNames = new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            columnNames.replaceAll(s -> RelationSchema.getFullName(tableName, s));
            fullColumnNamesList.add(columnNames);

            List<String> fullColumnNames =
                new ArrayList<>(Arrays.asList(entry.getValue().split(", ")));
            fullColumnNames.replaceAll(s -> RelationSchema.getQuotFullName(tableName, s));
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

          String filterStr = FilterTransformer.toString(copyFilter);
          statement =
              String.format(
                  CONCAT_QUERY_STATEMENT_AND_CONCAT_KEY,
                  allColumnNames,
                  fullColumnNames,
                  fullTableName,
                  filterStr.isEmpty() ? "" : "WHERE " + filterStr,
                  allColumnNames);

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
                  getDataTypeTransformer()));
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute project task in %s failure", getEngineName()), e));
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
          Connection defaultConn =
              getConnection(getDefaultDatabaseName()); // 正在使用的数据库无法被删除，因此需要切换到默认数据库
          if (defaultConn != null) {
            stmt = defaultConn.createStatement();
            statement = String.format(DROP_DATABASE_STATEMENT, databaseName);
            LOGGER.info("[Delete] execute delete: {}", statement);
            stmt.execute(statement); // 删除数据库
            stmt.close();
            defaultConn.close();
            return new TaskExecuteResult(null, null);
          } else {
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException(
                    String.format("cannot connect to database %s", getDefaultDatabaseName()),
                    new SQLException()));
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
              LOGGER.info("[Delete] execute delete: {}", statement);
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
              LOGGER.info("[Delete] execute delete: {}", statement);
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
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute delete task in %s failure", getEngineName()), e));
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
    try {
      conn.close();
    } catch (SQLException ex) {
      LOGGER.error("encounter error when closing connection: {}", ex.getMessage());
    }
    if (e != null) {
      return new TaskExecuteResult(
          null,
          new PhysicalException(
              String.format("execute insert task in %s failure", getEngineName()), e));
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
        }
        tableSet.close();
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.error("encounter error when getting boundary of storage: ", e);
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

    return new Pair<>(columnsInterval, new KeyInterval(Long.MIN_VALUE, Long.MAX_VALUE));
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
          RelationSchema schema = new RelationSchema(pattern);
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
      ResultSet rs =
          conn.getMetaData()
              .getColumns(
                  databaseName,
                  "public",
                  StringUtils.reformatPath(tableName),
                  StringUtils.reformatPath(columnNames));

      List<Pattern> patternList = getRegexPatternByName(tableName, columnNames, false);
      Pattern tableNamePattern = patternList.get(0), columnNamePattern = patternList.get(1);

      while (rs.next()) {
        String curTableName = rs.getString("TABLE_NAME");
        String curColumnNames = rs.getString("COLUMN_NAME");
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
          RelationSchema schema = new RelationSchema(pattern, true);
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
            if (!tableNamePattern.matcher(tempTableName).find()) {
              continue;
            }
            ResultSet columnSet =
                databaseMetaData.getColumns(tempDatabaseName, "public", tempTableName, columnNames);
            while (columnSet.next()) {
              String tempColumnNames = columnSet.getString("COLUMN_NAME");
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
          conn.close();
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
        conn.close();
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
      RelationSchema schema = new RelationSchema(path);
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
                  getDataTypeTransformer().toEngineType(dataType));
          LOGGER.info("[Create] execute create: {}", statement);
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
                    getDataTypeTransformer().toEngineType(dataType));
            LOGGER.info("[Create] execute create: {}", statement);
            stmt.execute(statement);
          }
          columnSet.close();
        }
        tableSet.close();
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
            RelationSchema schema = new RelationSchema(path);
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
          RelationSchema schema = new RelationSchema(path);
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

      //            LOGGER.info("[Insert] execute insert: {}", statement);
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
          RelationSchema schema = new RelationSchema(fullPath);
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
