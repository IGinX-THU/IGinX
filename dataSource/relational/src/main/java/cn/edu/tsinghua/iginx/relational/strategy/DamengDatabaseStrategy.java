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
package cn.edu.tsinghua.iginx.relational.strategy;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.DamengDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.zaxxer.hikari.HikariConfig;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengDatabaseStrategy extends AbstractDatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(DamengDatabaseStrategy.class);

  private final DamengDataTypeTransformer dataTypeTransformer;

  private static AbstractRelationalMeta checkAndSetPrivileges(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    Map<String, String> extraParams = storageEngineMeta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);
    // 判断达梦用户是否具有创建模式的权限
    try {
      Class.forName(relationalMeta.getDriverClass());
      Connection conn =
          DriverManager.getConnection(
              String.format(
                  "jdbc:dm://%s:%s?user=%s&password=\"%s\"",
                  storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password));
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(relationalMeta.getQueryUserPrivilegesStatement());
      List<String> databaseCreatePrivileges = relationalMeta.getDatabaseCreatePrivileges();
      while (rs.next()) {
        String privilege = rs.getString(1);
        if (databaseCreatePrivileges.contains(privilege)) {
          LOGGER.info("User {} has create database privilege", username);
          relationalMeta.setSupportCreateDatabase(true);
          break;
        }
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return relationalMeta;
  }

  public DamengDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(checkAndSetPrivileges(relationalMeta, storageEngineMeta), storageEngineMeta);
    this.dataTypeTransformer = DamengDataTypeTransformer.getInstance();
  }

  @Override
  public String getUrl(String databaseName, StorageEngineMeta meta) {
    return getConnectUrl(meta);
  }

  @Override
  public String getConnectUrl(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);

    return String.format(
        "jdbc:dm://%s:%s?user=%s&password=\"%s\"",
        meta.getIp(), meta.getPort(), username, password);
  }

  @Override
  public void configureDataSource(
      HikariConfig config, String databaseName, StorageEngineMeta meta) {
    config.setUsername(null);
    config.setPassword(null);
    // 一个用户对应一个SCHEMA，用户名只能大写，达梦设置为大小写不敏感，SCHEMA也只能为大写
    if (!databaseName.isEmpty()) {
      config.setConnectionInitSql("SET SCHEMA " + getQuotName(databaseName.toUpperCase()));
    }
  }

  @Override
  public String getDatabasePattern(String databaseName, boolean isDummy) {
    return null;
  }

  @Override
  public String getSchemaPattern(String databaseName, boolean isDummy) {
    if (isDummy || relationalMeta.supportCreateDatabase()) {
      return databaseName;
    }
    return storageEngineMeta.getExtraParams().get(USERNAME);
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
      if (relationalMeta.supportCreateDatabase()) {
        Map<String, ColumnField> columnMap = getColumnMap(conn, tableName, databaseName);
        this.batchInsert(conn, tableName, columnMap, parts, values);
      } else {
        Map<String, ColumnField> columnMap =
            getColumnMap(conn, databaseName + "." + tableName, null);
        this.batchInsert(conn, databaseName + "." + tableName, columnMap, parts, values);
      }
    }
    stmt.executeBatch();
  }

  private void batchInsert(
      Connection conn,
      String tableName,
      Map<String, ColumnField> columnMap,
      String[] parts,
      List<String> values) {
    Map<String, String[]> valueMap =
        values.stream()
            .map(value -> value.substring(0, value.length() - 2))
            .map(DamengDatabaseStrategy::splitByCommaWithQuotes)
            .collect(Collectors.toMap(arr -> arr[0], arr -> arr));
    List<String> allKeys = new ArrayList<>(valueMap.keySet());
    List<String> insertKeys = new ArrayList<>();
    List<String> updateKeys = new ArrayList<>();
    try {
      StringBuilder placeHolder = new StringBuilder();

      int start = 0, end = 0, step = 0;

      while (end < allKeys.size()) {
        step = Math.min(allKeys.size() - end, 500);
        end += step;
        IntStream.range(start, end)
            .forEach(
                i -> {
                  placeHolder.append("?,");
                });
        PreparedStatement selectStmt =
            conn.prepareStatement(
                String.format(
                    relationalMeta.getQueryTableStatement(),
                    getQuotName(KEY_NAME),
                    1,
                    getQuotName(tableName),
                    " WHERE "
                        + getQuotName(KEY_NAME)
                        + "IN ("
                        + placeHolder.substring(0, placeHolder.length() - 1)
                        + ")",
                    getQuotName(KEY_NAME)));
        for (int i = 0; i < end - start; i++) {
          selectStmt.setString(i + 1, allKeys.get(start + i));
        }
        ResultSet resultSet = selectStmt.executeQuery();
        while (resultSet.next()) {
          updateKeys.add(resultSet.getString(1));
        }
        start = end;
        placeHolder.setLength(0);
        resultSet.close();
        selectStmt.close();
      }
      insertKeys =
          allKeys.stream().filter(item -> !updateKeys.contains(item)).collect(Collectors.toList());

      // insert
      placeHolder.setLength(0);
      Arrays.stream(parts).forEach(part -> placeHolder.append("?,"));
      placeHolder.append("?");
      String partStr = Arrays.stream(parts).map(this::getQuotName).collect(Collectors.joining(","));
      PreparedStatement insertStmt =
          conn.prepareStatement(
              String.format(
                  relationalMeta.getInsertTableStatement(),
                  getQuotName(tableName),
                  getQuotName(KEY_NAME) + "," + partStr,
                  placeHolder));
      conn.setAutoCommit(false); // 关闭自动提交
      for (int i = 0; i < insertKeys.size(); i++) {
        String[] vals = valueMap.get(insertKeys.get(i));
        insertStmt.setString(1, vals[0]);
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
        if (i % 500 == 0) { // 每1000条数据执行一次批处理
          insertStmt.executeBatch(); // 执行批处理
          insertStmt.clearBatch();
        }
      }
      insertStmt.executeBatch();
      insertStmt.close();
      conn.commit();

      // upadte  String updateSql = "UPDATE %s.%s SET %s WHERE %s = %s";
      placeHolder.setLength(0);
      Arrays.stream(parts).forEach(part -> placeHolder.append(getQuotName(part)).append("=?,"));
      PreparedStatement updateStmt =
          conn.prepareStatement(
              String.format(
                  relationalMeta.getUpdateTableStatement(),
                  getQuotName(tableName),
                  placeHolder.substring(0, placeHolder.length() - 1),
                  getQuotName(KEY_NAME),
                  "?"));
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
        updateStmt.setString(parts.length + 1, vals[0]);
        updateStmt.addBatch();
        if (i % 500 == 0) { // 每500条数据执行一次批处理
          updateStmt.executeBatch();
          updateStmt.clearBatch();
        }
      }
      updateStmt.executeBatch();
      updateStmt.close();
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    if (param.getType() == Expression.ExpressionType.Base) {
      return "%s(CAST(%s AS DECIMAL(34, 16)))";
    }
    return "%s(%s)";
  }

  public Map<String, ColumnField> getColumnMap(Connection conn, String tableName, String schemaName)
      throws SQLException {
    List<ColumnField> columnFieldList = getColumns(conn, tableName, schemaName);
    Set<String> seen = new HashSet<>();
    for (ColumnField field : columnFieldList) {
      String name = field.getColumnName();
      if (!seen.add(name)) {
        LOGGER.warn("Duplicate column name detected: {}", name);
      }
    }
    return columnFieldList.stream()
        .collect(Collectors.toMap(ColumnField::getColumnName, field -> field));
  }

  private List<ColumnField> getColumns(Connection conn, String tableName, String schemaName)
      throws SQLException {
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    try (ResultSet rs =
        databaseMetaData.getColumns(
            getDatabasePattern(null, false),
            getSchemaPattern(schemaName, false),
            tableName,
            null)) {
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

  private void setValue(PreparedStatement stmt, int index, String value, DataType type)
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
        } else if (value.startsWith("'") && value.endsWith("'")) { // 处理空字符串'', 非空字符串包含特殊字符的情况'""'
          stmt.setString(index, value.substring(1, value.length() - 1));
        } else {
          stmt.setString(index, value);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static String[] splitByCommaWithQuotes(String input) {
    // 引号中不包含逗号时，使用split方式返回
    String regex = "(['\"])(.*?)\\1";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(input);
    boolean containsCommaWithQuotes = false;
    while (matcher.find()) {
      if (matcher.group().contains(",")) {
        LOGGER.debug("Found: {} :: {}", matcher.group(), input);
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
}
