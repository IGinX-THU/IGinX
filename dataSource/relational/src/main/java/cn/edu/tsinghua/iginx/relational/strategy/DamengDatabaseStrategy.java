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
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengDatabaseStrategy implements DatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(DamengDatabaseStrategy.class);

  private AbstractRelationalMeta relationalMeta;

  public DamengDatabaseStrategy(AbstractRelationalMeta relationalMeta) {
    this.relationalMeta = relationalMeta;
  }

  @Override
  public String getQuotName(String name) {
    return String.format("%s%s%s", relationalMeta.getQuote(), name, relationalMeta.getQuote());
  }

  @Override
  public String getUrl(String databaseName, StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    return String.format(
        "jdbc:dm://%s:%s?user=%s&password=%s&schema=%s",
        meta.getIp(),
        meta.getPort(),
        extraParams.get(USERNAME),
        extraParams.get(PASSWORD),
        databaseName);
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
  public String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException {
    return rs.getString("TABLE_SCHEMA");
  }

  @Override
  public String getSchemaPattern(String databaseName) {
    return databaseName;
  }

  @Override
  public String formatConcatStatement(List<String> columns) {
    if (columns.size() == 1) {
      return String.format(" CONCAT(%s, '') ", columns.get(0));
    }
    return String.format(" CONCAT(%s) ", String.join(", ", columns));
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
      Map<String, String[]> valueMap = new HashMap<>();
      Map<String, ColumnField> columnMap = getColumnMap(conn, databaseName, tableName);
      for (String value : values) {
        String csvLine = value.substring(0, value.length() - 2);

        // 临时替换引号内的逗号
        StringBuilder processed = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < csvLine.length(); i++) {
          char c = csvLine.charAt(i);

          if (c == '\'') {
            inQuotes = !inQuotes;
          }

          if (c == ',' && inQuotes) {
            processed.append("##COMMA##");
          } else {
            processed.append(c);
          }
        }

        // 正常分割
        String[] value_parts = processed.toString().split(", ");

        // 恢复原来的逗号
        for (int i = 0; i < value_parts.length; i++) {
          value_parts[i] = value_parts[i].replace("##COMMA##", ",");
        }

        valueMap.put(value_parts[0], value_parts);
      }

      List<String> allKeys = new ArrayList<>(valueMap.keySet());
      List<String> insertKeys = new ArrayList<>();
      List<String> updateKeys = new ArrayList<>();
      try {
        StringBuilder placeHolder = new StringBuilder();

        int start = 0, end = 0, step = 0;

        while (end < allKeys.size()) {
          step = Math.min(allKeys.size() - end, 500);
          end += step;
          IntStream.range(start, end).forEach(i -> placeHolder.append("?,"));

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
            allKeys.stream()
                .filter(item -> !updateKeys.contains(item))
                .collect(Collectors.toList());

        // insert
        placeHolder.setLength(0);
        Arrays.stream(parts).forEach(part -> placeHolder.append("?,"));
        String partStr =
            Arrays.stream(parts).map(this::getQuotName).collect(Collectors.joining(","));
        PreparedStatement insertStmt =
            conn.prepareStatement(
                String.format(
                    relationalMeta.getInsertTableStatement(),
                    getQuotName(tableName),
                    getQuotName(KEY_NAME) + "," + partStr,
                    placeHolder.append("?")));

        conn.setAutoCommit(false); // 关闭自动提交
        for (int i = 0; i < insertKeys.size(); i++) {
          String[] vals = valueMap.get(insertKeys.get(i));
          insertStmt.setString(1, vals[0]);
          for (int j = 0; j < parts.length; j++) {
            if (!columnMap.containsKey(parts[j])) {
              break;
            }
            if (columnMap.get(parts[j]).columnTypeName.equals("NUMBER")) {
              int columnSize = columnMap.get(parts[j]).columnSize;
              if (columnSize == 1) {
                setValue(insertStmt, j + 2, vals[j + 1], Types.BOOLEAN);
              } else if (columnSize >= 1 && columnSize <= 10) {
                setValue(insertStmt, j + 2, vals[j + 1], Types.INTEGER);
              } else if (columnSize == 38) {
                setValue(insertStmt, j + 2, vals[j + 1], Types.DOUBLE);
              } else {
                setValue(insertStmt, j + 2, vals[j + 1], Types.BIGINT);
              }
            } else if (columnMap.get(parts[j]).columnTypeName.equals("FLOAT")) {
              setValue(insertStmt, j + 2, vals[j + 1], Types.FLOAT);
            } else if (columnMap.get(parts[j]).columnTypeName.equals("TINYINT")) {
              setValue(insertStmt, j + 2, vals[j + 1], Types.BOOLEAN);
            } else {
              setValue(insertStmt, j + 2, vals[j + 1], Types.VARCHAR);
            }
          }
          insertStmt.addBatch();
          if (i % 500 == 0) { // 每500条数据执行一次批处理
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
            if (columnMap.get(parts[j]).columnTypeName.equals("NUMBER")) {
              int columnSize = columnMap.get(parts[j]).columnSize;
              if (columnSize == 1) {
                setValue(updateStmt, j + 1, vals[j + 1], Types.BOOLEAN);
              } else if (columnSize >= 1 && columnSize <= 10) {
                setValue(updateStmt, j + 1, vals[j + 1], Types.INTEGER);
              } else if (columnSize == 38) {
                setValue(updateStmt, j + 1, vals[j + 1], Types.DOUBLE);
              } else {
                setValue(updateStmt, j + 1, vals[j + 1], Types.BIGINT);
              }
            } else if (columnMap.get(parts[j]).columnTypeName.equals("FLOAT")) {
              setValue(updateStmt, j + 1, vals[j + 1], Types.FLOAT);
            } else if (columnMap.get(parts[j]).columnTypeName.equals("TINYINT")) {
              setValue(updateStmt, j + 1, vals[j + 1], Types.BOOLEAN);
            } else {
              setValue(updateStmt, j + 1, vals[j + 1], Types.VARCHAR);
            }
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
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    if (param.getType() == Expression.ExpressionType.Base) {
      return "%s(CAST(%s AS DECIMAL(34, 16)))";
    }
    return "%s(%s)";
  }

  private Map<String, ColumnField> getColumnMap(
      Connection conn, String databaseName, String tableName) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    ResultSet rs = metaData.getColumns(databaseName, databaseName, tableName, null);
    Map<String, ColumnField> columnMap = new HashMap<>();

    while (rs.next()) {
      String columnName = rs.getString("COLUMN_NAME");
      String columnTypeName = rs.getString("TYPE_NAME");
      String columnTable = rs.getString("TABLE_NAME");
      int columnSize = rs.getInt("COLUMN_SIZE");
      int columnType = rs.getInt("DATA_TYPE");
      int decimalDigits = rs.getInt("DECIMAL_DIGITS");

      columnMap.put(columnName, new ColumnField(columnTable, columnName, columnType,columnTypeName,columnSize,decimalDigits));
    }

    rs.close();
    return columnMap;
  }

  private void setValue(PreparedStatement stmt, int index, String value, int types)
      throws SQLException {
    if (value.equals("null")) {
      if (Types.BOOLEAN == types) {
        stmt.setNull(index, Types.INTEGER);
      } else {
        stmt.setNull(index, types);
      }
      return;
    }
    switch (types) {
      case Types.BOOLEAN:
        stmt.setInt(index, value.equalsIgnoreCase("true") ? 1 : 0);
        break;
      case Types.INTEGER:
        stmt.setInt(index, Integer.parseInt(value));
        break;
      case Types.BIGINT:
        stmt.setLong(index, Long.parseLong(value));
        break;
      case Types.FLOAT:
        stmt.setFloat(index, Float.parseFloat(value));
        break;
      case Types.DOUBLE:
        stmt.setDouble(index, Double.parseDouble(value));
        break;
      default:
        if (value.startsWith("'") && value.endsWith("'")) {
          stmt.setString(index, value.substring(1, value.length() - 1));
        } else {
          stmt.setString(index, value);
        }
    }
  }
}
