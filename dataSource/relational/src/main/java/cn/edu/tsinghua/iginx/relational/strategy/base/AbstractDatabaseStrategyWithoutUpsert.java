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
package cn.edu.tsinghua.iginx.relational.strategy.base;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.USERNAME;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.relational.tools.SqlStringUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDatabaseStrategyWithoutUpsert extends AbstractDatabaseStrategy {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractDatabaseStrategyWithoutUpsert.class);

  protected final IDataTypeTransformer dataTypeTransformer;

  public AbstractDatabaseStrategyWithoutUpsert(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
    this.dataTypeTransformer = relationalMeta.getDataTypeTransformer();
  }

  /** 抽象方法：获取列元数据。 子类必须实现此方法以处理特定数据库的 Schema/Database 检索逻辑。 */
  public Map<String, ColumnField> getColumnMap(
      Connection conn, String databaseName, String tableName) throws SQLException {
    return null;
  }

  @Override
  public void executeBatchInsert(
      Connection conn,
      String databaseName,
      Statement stmt,
      Map<String, Pair<String, List<String>>> tableToColumnEntries,
      char quote)
      throws SQLException {
    for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
      String tableName = entry.getKey();
      String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
      List<String> values = entry.getValue().v;
      String[] parts = columnNames.split(", ");

      // 获取用于 SQL 执行的完整表名
      String fullTableName = getFullTableName(databaseName, tableName);

      // 获取元数据映射 (委托给子类处理特定的 Schema 逻辑)
      Map<String, ColumnField> columnMap = getColumnMap(conn, databaseName, tableName);

      this.doManualUpsert(conn, fullTableName, columnMap, parts, values);
    }
    stmt.executeBatch();
  }

  /** 模拟 Upsert 的核心逻辑： 1. 查询哪些 Key 已经存在。 2. 批量插入 (Insert) 不存在的 Key。 3. 批量更新 (Update) 已存在的 Key。 */
  protected void doManualUpsert(
      Connection conn,
      String fullTableName,
      Map<String, ColumnField> columnMap,
      String[] parts,
      List<String> values)
      throws SQLException {

    // 将所有原始值字符串解析为数组
    Map<String, String[]> valueMap =
        values.stream()
            .map(value -> value.substring(0, value.length() - 2))
            .map(AbstractDatabaseStrategyWithoutUpsert::splitByCommaWithQuotes)
            .collect(Collectors.toMap(arr -> arr[0], arr -> arr));

    List<String> allKeys = new ArrayList<>(valueMap.keySet());
    List<String> updateKeys = new ArrayList<>();

    // 步骤 1: 查询已存在的 Key
    StringBuilder placeHolder = new StringBuilder();
    int start = 0, end = 0, step;

    while (end < allKeys.size()) {
      step = Math.min(allKeys.size() - end, 500);
      end += step;
      IntStream.range(start, end).forEach(i -> placeHolder.append("?,"));

      String querySql =
          String.format(
              relationalMeta.getQueryTableStatement(),
              getQuotName(KEY_NAME),
              1,
              fullTableName,
              " WHERE "
                  + getQuotName(KEY_NAME)
                  + " IN ("
                  + placeHolder.substring(0, placeHolder.length() - 1)
                  + ")",
              getQuotName(KEY_NAME));

      try (PreparedStatement selectStmt = conn.prepareStatement(querySql)) {
        for (int i = 0; i < end - start; i++) {
          selectStmt.setString(i + 1, allKeys.get(start + i));
        }
        try (ResultSet resultSet = selectStmt.executeQuery()) {
          while (resultSet.next()) {
            updateKeys.add(resultSet.getString(1));
          }
        }
      }
      start = end;
      placeHolder.setLength(0);
    }

    List<String> insertKeys =
        allKeys.stream().filter(item -> !updateKeys.contains(item)).collect(Collectors.toList());

    boolean autoCommit = conn.getAutoCommit();
    conn.setAutoCommit(false); // 关闭自动提交以进行批处理

    try {
      // 步骤 2: 批量插入 (Insert)
      if (!insertKeys.isEmpty()) {
        placeHolder.setLength(0);
        Arrays.stream(parts).forEach(part -> placeHolder.append("?,"));
        placeHolder.append("?"); // 为 Key 占位

        String partStr =
            Arrays.stream(parts).map(this::getQuotName).collect(Collectors.joining(","));
        String insertSql =
            String.format(
                relationalMeta.getInsertTableStatement(),
                fullTableName,
                getQuotName(KEY_NAME) + "," + partStr,
                placeHolder.toString());

        try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
          for (int i = 0; i < insertKeys.size(); i++) {
            String[] vals = valueMap.get(insertKeys.get(i));
            insertStmt.setString(1, vals[0]); // Key

            for (int j = 0; j < parts.length; j++) {
              if (!columnMap.containsKey(parts[j])) {
                break;
              }
              ColumnField columnField = columnMap.get(parts[j]);
              DataType dataType =
                  dataTypeTransformer.fromEngineType(
                      columnField.getColumnType(),
                      columnField.getColumnSize(),
                      columnField.getDecimalDigits());
              setValue(insertStmt, j + 2, vals[j + 1], dataType);
            }
            insertStmt.addBatch();
            if (i > 0 && i % 500 == 0) {
              insertStmt.executeBatch();
              insertStmt.clearBatch();
            }
          }
          insertStmt.executeBatch();
        }
        conn.commit();
      }

      // 步骤 3: 批量更新 (Update)
      if (!updateKeys.isEmpty()) {
        placeHolder.setLength(0);
        Arrays.stream(parts).forEach(part -> placeHolder.append(getQuotName(part)).append("=?,"));

        String updateSql =
            String.format(
                relationalMeta.getUpdateTableStatement(),
                fullTableName,
                placeHolder.substring(0, placeHolder.length() - 1),
                getQuotName(KEY_NAME),
                "?");

        try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
          for (int i = 0; i < updateKeys.size(); i++) {
            String[] vals = valueMap.get(updateKeys.get(i));
            for (int j = 0; j < parts.length; j++) {
              if (!columnMap.containsKey(parts[j])) {
                break;
              }
              ColumnField columnField = columnMap.get(parts[j]);
              DataType dataType =
                  dataTypeTransformer.fromEngineType(
                      columnField.getColumnType(),
                      columnField.getColumnSize(),
                      columnField.getDecimalDigits());
              setValue(updateStmt, j + 1, vals[j + 1], dataType);
            }
            updateStmt.setString(parts.length + 1, vals[0]); // Where Key = ?
            updateStmt.addBatch();
            if (i > 0 && i % 500 == 0) {
              updateStmt.executeBatch();
              updateStmt.clearBatch();
            }
          }
          updateStmt.executeBatch();
        }
        conn.commit();
      }
    } finally {
      conn.setAutoCommit(autoCommit);
    }
  }

  protected String getFullTableName(String databaseName, String tableName) {
    if (tableName == null) {
      return null;
    }
    if (databaseName == null
        || databaseName.isEmpty()
        || relationalMeta.isSupportCreateDatabase()) {
      return getQuotName(tableName);
    }
    return getQuotName(databaseName + GlobalConstant.DOT + tableName);
  }

  protected void setValue(PreparedStatement stmt, int index, String value, DataType type)
      throws SQLException {
    boolean isNull = value.equals("null");
    switch (type) {
      case BOOLEAN:
        if (isNull) {
          stmt.setNull(index, Types.INTEGER);
        } else {
          stmt.setInt(index, value.equalsIgnoreCase("true") ? 1 : 0);
        }
        break;
      case INTEGER:
        if (isNull) {
          stmt.setNull(index, Types.INTEGER);
        } else {
          stmt.setInt(index, Integer.parseInt(value));
        }
        break;
      case LONG:
        if (isNull) {
          stmt.setNull(index, Types.BIGINT);
        } else {
          stmt.setLong(index, Long.parseLong(value));
        }
        break;
      case FLOAT:
        if (isNull) {
          stmt.setNull(index, Types.FLOAT);
        } else {
          stmt.setFloat(index, Float.parseFloat(value));
        }
        break;
      case DOUBLE:
        if (isNull) {
          stmt.setNull(index, Types.DOUBLE);
        } else {
          stmt.setDouble(index, Double.parseDouble(value));
        }
        break;
      case BINARY:
        if (isNull) {
          stmt.setNull(index, Types.VARCHAR);
        } else if (value.startsWith("'") && value.endsWith("'")) {
          String literal = value.substring(1, value.length() - 1);
          // 在Dameng和Oracle中，insert采用的是PreparedStatement占位符插入的方式，不需要进行转义，把转义预处理去掉
          if (relationalMeta.isStringLiteralBackslashEscape()) {
            literal = literal.replace("\\\\", "\\");
          }
          literal = literal.replace("''", "'");
          stmt.setString(index, literal);
        } else {
          stmt.setString(index, value);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  protected static String[] splitByCommaWithQuotes(String input) {
    String regex = "(['\"])(.*?)\\1";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(input);
    boolean containsCommaWithQuotes = false;
    while (matcher.find()) {
      if (matcher.group().contains(",")) {
        containsCommaWithQuotes = true;
        break;
      }
    }
    if (!containsCommaWithQuotes) {
      return Arrays.stream(input.split(",")).map(String::trim).toArray(String[]::new);
    }

    List<String> resultList = new ArrayList<>();
    StringBuilder currentPart = new StringBuilder();
    boolean insideQuotes = false;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == ',' && !insideQuotes) {
        resultList.add(currentPart.toString().trim());
        currentPart.setLength(0);
      } else if (c == '\'' || c == '\"') {
        insideQuotes = !insideQuotes;
        currentPart.append(c);
      } else {
        currentPart.append(c);
      }
    }
    if (currentPart.length() > 0) {
      resultList.add(currentPart.toString().trim());
    }
    return resultList.toArray(new String[0]);
  }

  public List<ColumnField> getColumns(
      Connection conn, String schemaPattern, String tableNamePattern) throws SQLException {
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    // 大多数 JDBC 驱动 catalog 传 null，schema 传 pattern，tableName 传 pattern
    try (ResultSet rs = databaseMetaData.getColumns(null, schemaPattern, tableNamePattern, null)) {
      List<ColumnField> columnFields = new ArrayList<>();
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        String columnType = rs.getString("TYPE_NAME");
        String columnTable = rs.getString("TABLE_NAME");
        int columnSize = rs.getInt("COLUMN_SIZE");
        int decimalDigits = rs.getInt("DECIMAL_DIGITS");
        columnFields.add(
            new ColumnField(columnTable, columnName, columnType, columnSize, decimalDigits));
      }
      return columnFields;
    }
  }

  @Override
  public ColumnsInterval getColumnsBoundary() throws PhysicalException, SQLException {
    String defaultDb = relationalMeta.getDefaultDatabaseName();
    String columnNames = "owner, table_name, column_name";
    List<String> exceptSchema = new ArrayList<>();
    exceptSchema.add(relationalMeta.getDefaultDatabaseName());
    exceptSchema.addAll(relationalMeta.getSystemDatabaseName());

    // 注意：这里使用了 Oracle/Dameng 特有的 owner 字段和 all_tables 视图
    String conditionStatement =
        exceptSchema.stream()
            .map(s -> "'" + SqlStringUtils.escapeSqlQuotedContent(s, '\'') + "'")
            .collect(Collectors.joining(", ", " WHERE owner NOT IN (", ")"));

    if (boundaryLevel < 1) {
      String sql = "SELECT min(owner), max(owner) FROM all_tables " + conditionStatement;
      try (Connection conn = getConnection(defaultDb);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        if (rs.next()) {
          String minPath = rs.getString(1);
          String maxPath = rs.getString(2);
          return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
        } else {
          throw new RelationalTaskExecuteFailureException("no data!");
        }
      }
    }
    String sqlMin =
        "SELECT "
            + columnNames
            + " FROM all_tab_columns"
            + conditionStatement
            + " ORDER BY owner, table_name, column_name LIMIT 1";
    String sqlMax =
        "SELECT "
            + columnNames
            + " FROM all_tab_columns"
            + conditionStatement
            + " ORDER BY owner DESC, table_name DESC, column_name DESC LIMIT 1";

    String minPath = null;
    String maxPath = null;
    try (Connection conn = getConnection(defaultDb);
        Statement statement = conn.createStatement()) {
      try (ResultSet rs = statement.executeQuery(sqlMin)) {
        if (rs.next()) {
          minPath = rs.getString(1) + DOT + rs.getString(2) + DOT + rs.getString(3);
        }
      }
      try (ResultSet rs = statement.executeQuery(sqlMax)) {
        if (rs.next()) {
          maxPath = rs.getString(1) + DOT + rs.getString(2) + DOT + rs.getString(3);
        }
      }
    }
    minPath = minPath == null ? defaultDb : minPath;
    maxPath = maxPath == null ? defaultDb : maxPath;
    return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  }

  @Override
  public String getDatabasePattern(String databaseName, boolean isDummy) {
    return null;
  }

  @Override
  public String getSchemaPattern(String databaseName, boolean isDummy) {
    if (isDummy || relationalMeta.isSupportCreateDatabase()) {
      return databaseName;
    }
    return storageEngineMeta.getExtraParams().get(USERNAME);
  }
}
