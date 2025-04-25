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

  public DamengDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
    this.dataTypeTransformer = DamengDataTypeTransformer.getInstance();
  }

  @Override
  public String getUrl(String databaseName, StorageEngineMeta meta) {
    return getConnectUrl(meta);
    //    Map<String, String> extraParams = meta.getExtraParams();
    //    return String.format(
    //        "jdbc:dm://%s:%s?user=%s&password=%s&schema=%s",
    //        meta.getIp(),
    //        meta.getPort(),
    //        extraParams.get(USERNAME),
    //        extraParams.get(PASSWORD),
    //        databaseName);
  }

  @Override
  public String getConnectUrl(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);

    return String.format(
        "jdbc:dm://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(), username, password);
  }

  @Override
  public void configureDataSource(
      HikariConfig config, String databaseName, StorageEngineMeta meta) {
    config.setUsername(null);
    config.setPassword(null);
    if (!databaseName.isEmpty()) {
      config.setConnectionInitSql("SET SCHEMA " + getQuotName(databaseName));
    }
  }

  //  @Override
  //  public String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException {
  //    return rs.getString("TABLE_SCHEMA");
  //  }

  @Override
  public String getDatabasePattern(String databaseName, boolean isDummy) {
    return null;
  }

  @Override
  public String getSchemaPattern(String databaseName, boolean isDummy) {
    if (isDummy) {
      return databaseName;
    }
    return storageEngineMeta.getExtraParams().get(USERNAME);
  }

  //  @Override
  //  public String formatConcatStatement(List<String> columns) {
  //    if (columns.size() == 1) {
  //      return String.format(" CONCAT(%s, '') ", columns.get(0));
  //    }
  //    return String.format(" CONCAT(%s) ", String.join(", ", columns));
  //  }

  //    @Override
  //    public void executeBatchInsert(
  //        Connection conn,
  //        String databaseName,
  //        Statement stmt,
  //        Map<String, Pair<String, List<String>>> tableToColumnEntries,
  //        char quote)
  //        throws SQLException {
  //      for (Map.Entry<String, Pair<String, List<String>>> entry :
  // tableToColumnEntries.entrySet())
  //   {
  //        String tableName = entry.getKey();
  //        String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
  //        List<String> values = entry.getValue().v;
  //        String[] parts = columnNames.split(", ");
  //        Map<String, String[]> valueMap = new HashMap<>();
  //        Map<String, ColumnField> columnMap = getColumnMap(conn, databaseName, tableName);
  //        for (String value : values) {
  //          String csvLine = value.substring(0, value.length() - 2);
  //
  //          // 临时替换引号内的逗号
  //          StringBuilder processed = new StringBuilder();
  //          boolean inQuotes = false;
  //          for (int i = 0; i < csvLine.length(); i++) {
  //            char c = csvLine.charAt(i);
  //
  //            if (c == '\'') {
  //              inQuotes = !inQuotes;
  //            }
  //
  //            if (c == ',' && inQuotes) {
  //              processed.append("##COMMA##");
  //            } else {
  //              processed.append(c);
  //            }
  //          }
  //
  //          // 正常分割
  //          String[] value_parts = processed.toString().split(", ");
  //
  //          // 恢复原来的逗号
  //          for (int i = 0; i < value_parts.length; i++) {
  //            value_parts[i] = value_parts[i].replace("##COMMA##", ",");
  //          }
  //
  //          valueMap.put(value_parts[0], value_parts);
  //        }
  //
  //        List<String> allKeys = new ArrayList<>(valueMap.keySet());
  //        List<String> insertKeys = new ArrayList<>();
  //        List<String> updateKeys = new ArrayList<>();
  //        try {
  //          StringBuilder placeHolder = new StringBuilder();
  //
  //          int start = 0, end = 0, step = 0;
  //
  //          while (end < allKeys.size()) {
  //            step = Math.min(allKeys.size() - end, 500);
  //            end += step;
  //            IntStream.range(start, end).forEach(i -> placeHolder.append("?,"));
  //
  //            PreparedStatement selectStmt =
  //                conn.prepareStatement(
  //                    String.format(
  //                        relationalMeta.getQueryTableStatement(),
  //                        getQuotName(KEY_NAME),
  //                        1,
  //                        getQuotName(tableName),
  //                        " WHERE "
  //                            + getQuotName(KEY_NAME)
  //                            + "IN ("
  //                            + placeHolder.substring(0, placeHolder.length() - 1)
  //                            + ")",
  //                        getQuotName(KEY_NAME)));
  //
  //            for (int i = 0; i < end - start; i++) {
  //              selectStmt.setString(i + 1, allKeys.get(start + i));
  //            }
  //            ResultSet resultSet = selectStmt.executeQuery();
  //            while (resultSet.next()) {
  //              updateKeys.add(resultSet.getString(1));
  //            }
  //            start = end;
  //            placeHolder.setLength(0);
  //            resultSet.close();
  //            selectStmt.close();
  //          }
  //          insertKeys =
  //              allKeys.stream()
  //                  .filter(item -> !updateKeys.contains(item))
  //                  .collect(Collectors.toList());
  //
  //          // insert
  //          placeHolder.setLength(0);
  //          Arrays.stream(parts).forEach(part -> placeHolder.append("?,"));
  //          String partStr =
  //              Arrays.stream(parts).map(this::getQuotName).collect(Collectors.joining(","));
  //          PreparedStatement insertStmt =
  //              conn.prepareStatement(
  //                  String.format(
  //                      relationalMeta.getInsertTableStatement(),
  //                      getQuotName(tableName),
  //                      getQuotName(KEY_NAME) + "," + partStr,
  //                      placeHolder.append("?")));
  //
  //          conn.setAutoCommit(false); // 关闭自动提交
  //          for (int i = 0; i < insertKeys.size(); i++) {
  //            String[] vals = valueMap.get(insertKeys.get(i));
  //            insertStmt.setString(1, vals[0]);
  //            for (int j = 0; j < parts.length; j++) {
  //              if (!columnMap.containsKey(parts[j])) {
  //                break;
  //              }
  //              if (columnMap.get(parts[j]).getColumnType().equals("NUMBER")) {
  //                int columnSize = columnMap.get(parts[j]).getColumnSize();
  //                if (columnSize == 1) {
  //                  setValue(insertStmt, j + 2, vals[j + 1], Types.BOOLEAN);
  //                } else if (columnSize >= 1 && columnSize <= 10) {
  //                  setValue(insertStmt, j + 2, vals[j + 1], Types.INTEGER);
  //                } else if (columnSize == 38) {
  //                  setValue(insertStmt, j + 2, vals[j + 1], Types.DOUBLE);
  //                } else {
  //                  setValue(insertStmt, j + 2, vals[j + 1], Types.BIGINT);
  //                }
  //              } else if (columnMap.get(parts[j]).getColumnType().equals("FLOAT")) {
  //                setValue(insertStmt, j + 2, vals[j + 1], Types.FLOAT);
  //              } else if (columnMap.get(parts[j]).getColumnType().equals("TINYINT")) {
  //                setValue(insertStmt, j + 2, vals[j + 1], Types.BOOLEAN);
  //              } else {
  //                setValue(insertStmt, j + 2, vals[j + 1], Types.VARCHAR);
  //              }
  //            }
  //            insertStmt.addBatch();
  //            if (i % 500 == 0) { // 每500条数据执行一次批处理
  //              insertStmt.executeBatch(); // 执行批处理
  //              insertStmt.clearBatch();
  //            }
  //          }
  //          insertStmt.executeBatch();
  //          insertStmt.close();
  //          conn.commit();
  //
  //          // upadte  String updateSql = "UPDATE %s.%s SET %s WHERE %s = %s";
  //          placeHolder.setLength(0);
  //          Arrays.stream(parts).forEach(part ->
  //   placeHolder.append(getQuotName(part)).append("=?,"));
  //          PreparedStatement updateStmt =
  //              conn.prepareStatement(
  //                  String.format(
  //                      relationalMeta.getUpdateTableStatement(),
  //                      getQuotName(tableName),
  //                      placeHolder.substring(0, placeHolder.length() - 1),
  //                      getQuotName(KEY_NAME),
  //                      "?"));
  //
  //          for (int i = 0; i < updateKeys.size(); i++) {
  //            String[] vals = valueMap.get(updateKeys.get(i));
  //            for (int j = 0; j < parts.length; j++) {
  //              if (!columnMap.containsKey(parts[j])) {
  //                break;
  //              }
  //              if (columnMap.get(parts[j]).getColumnType().equals("NUMBER")) {
  //                int columnSize = columnMap.get(parts[j]).getColumnSize();
  //                if (columnSize == 1) {
  //                  setValue(updateStmt, j + 1, vals[j + 1], Types.BOOLEAN);
  //                } else if (columnSize >= 1 && columnSize <= 10) {
  //                  setValue(updateStmt, j + 1, vals[j + 1], Types.INTEGER);
  //                } else if (columnSize == 38) {
  //                  setValue(updateStmt, j + 1, vals[j + 1], Types.DOUBLE);
  //                } else {
  //                  setValue(updateStmt, j + 1, vals[j + 1], Types.BIGINT);
  //                }
  //              } else if (columnMap.get(parts[j]).getColumnType().equals("FLOAT")) {
  //                setValue(updateStmt, j + 1, vals[j + 1], Types.FLOAT);
  //              } else if (columnMap.get(parts[j]).getColumnType().equals("TINYINT")) {
  //                setValue(updateStmt, j + 1, vals[j + 1], Types.BOOLEAN);
  //              } else {
  //                setValue(updateStmt, j + 1, vals[j + 1], Types.VARCHAR);
  //              }
  //            }
  //            updateStmt.setString(parts.length + 1, vals[0]);
  //            updateStmt.addBatch();
  //            if (i % 500 == 0) { // 每500条数据执行一次批处理
  //              updateStmt.executeBatch();
  //              updateStmt.clearBatch();
  //            }
  //          }
  //          updateStmt.executeBatch();
  //          updateStmt.close();
  //          conn.commit();
  //        } catch (SQLException e) {
  //          throw new RuntimeException(e);
  //        }
  //      }
  //    }

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
      Map<String, ColumnField> columnMap = getColumnMap(conn, databaseName + "." + tableName);
      this.batchInsert(conn, databaseName + "." + tableName, columnMap, parts, values);
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

  //  private Map<String, ColumnField> getColumnMap(
  //      Connection conn, String tableName) throws SQLException {
  ////    DatabaseMetaData metaData = conn.getMetaData();
  //    ResultSet rs = getColumns(conn, tableName);
  //    Map<String, ColumnField> columnMap = new HashMap<>();
  //
  //    while (rs.next()) {
  //      String columnName = rs.getString("COLUMN_NAME");
  //      String columnType = rs.getString("TYPE_NAME");
  //      String columnTable = rs.getString("TABLE_NAME");
  //      int columnSize = rs.getInt("COLUMN_SIZE");
  //      int decimalDigits = rs.getInt("DECIMAL_DIGITS");
  //
  //      columnMap.put(
  //          columnName,
  //          new ColumnField(columnTable, columnName, columnType, columnSize, decimalDigits));
  //    }
  //
  //    rs.close();
  //    return columnMap;
  //  }

  public Map<String, ColumnField> getColumnMap(Connection conn, String tableName)
      throws SQLException {
    List<ColumnField> columnFieldList = getColumns(conn, tableName);
    return columnFieldList.stream()
        .collect(Collectors.toMap(ColumnField::getColumnName, field -> field));
  }

  private List<ColumnField> getColumns(Connection conn, String tableName) throws SQLException {
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    try (ResultSet rs =
        databaseMetaData.getColumns(
            getDatabasePattern(null, false), getSchemaPattern(null, false), tableName, null)) {
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

  //  private void setValue(PreparedStatement stmt, int index, String value, int types)
  //      throws SQLException {
  //    if (value.equals("null")) {
  //      if (Types.BOOLEAN == types) {
  //        stmt.setNull(index, Types.INTEGER);
  //      } else {
  //        stmt.setNull(index, types);
  //      }
  //      return;
  //    }
  //    switch (types) {
  //      case Types.BOOLEAN:
  //        stmt.setInt(index, value.equalsIgnoreCase("true") ? 1 : 0);
  //        break;
  //      case Types.INTEGER:
  //        stmt.setInt(index, Integer.parseInt(value));
  //        break;
  //      case Types.BIGINT:
  //        stmt.setLong(index, Long.parseLong(value));
  //        break;
  //      case Types.FLOAT:
  //        stmt.setFloat(index, Float.parseFloat(value));
  //        break;
  //      case Types.DOUBLE:
  //        stmt.setDouble(index, Double.parseDouble(value));
  //        break;
  //      default:
  //        if (value.startsWith("'") && value.endsWith("'")) {
  //          stmt.setString(index, value.substring(1, value.length() - 1));
  //        } else {
  //          stmt.setString(index, value);
  //        }
  //    }
  //  }

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
