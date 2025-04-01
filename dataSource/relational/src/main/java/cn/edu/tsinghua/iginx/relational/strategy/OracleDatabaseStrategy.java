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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.RelationalStorage;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDatabaseStrategy implements DatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(DamengDatabaseStrategy.class);

  private AbstractRelationalMeta relationalMeta;

  public OracleDatabaseStrategy(AbstractRelationalMeta relationalMeta) {
    this.relationalMeta = relationalMeta;
  }

  @Override
  public String getQuotName(String name) {
    return String.format("%s%s%s", relationalMeta.getQuote(), name, relationalMeta.getQuote());
  }

  @Override
  public String getUrl(String databaseName, StorageEngineMeta meta) {
    return String.format(
        "jdbc:oracle:thin:@//%s:%d/%s",
        meta.getIp(), meta.getPort(), relationalMeta.getDefaultDatabaseName());
  }

  @Override
  public String getConnectUrl(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);

    return String.format(
        "jdbc:oracle:thin:%s/%s@%s:%d/%s",
        username, password, meta.getIp(), meta.getPort(), relationalMeta.getDefaultDatabaseName());
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
      Map<String, ColumnField> columnMap = getColumnMap(conn, databaseName, tableName);
      this.batchInsert(conn, databaseName, tableName, columnMap, parts, values);
    }
    stmt.executeBatch();
  }

  private void batchInsert(
      Connection conn,
      String databaseName,
      String tableName,
      Map<String, ColumnField> columnMap,
      String[] parts,
      List<String> values) {
    Map<String, String[]> valueMap =
        values.stream()
            .map(value -> value.substring(0, value.length() - 2))
            .map(RelationalStorage::splitByCommaWithQuotes)
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
                    getTableNameByDB(databaseName, tableName),
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
      String partStr = Arrays.stream(parts).map(this::getQuotName).collect(Collectors.joining(","));
      PreparedStatement insertStmt =
          conn.prepareStatement(
              String.format(
                  relationalMeta.getInsertTableStatement(),
                  getTableNameByDB(databaseName, tableName),
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
          if (columnMap.get(parts[j]).columnType.equals("NUMBER")) {
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
          } else if (columnMap.get(parts[j]).columnType.equals("FLOAT")) {
            setValue(insertStmt, j + 2, vals[j + 1], Types.FLOAT);
          } else {
            setValue(insertStmt, j + 2, vals[j + 1], Types.VARCHAR);
          }
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
                  getTableNameByDB(databaseName, tableName),
                  placeHolder.substring(0, placeHolder.length() - 1),
                  getQuotName(KEY_NAME),
                  "?"));
      for (int i = 0; i < updateKeys.size(); i++) {
        String[] vals = valueMap.get(updateKeys.get(i));
        for (int j = 0; j < parts.length; j++) {
          if (!columnMap.containsKey(parts[j])) {
            break;
          }
          if (columnMap.get(parts[j]).columnType.equals("NUMBER")) {
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
          } else if (columnMap.get(parts[j]).columnType.equals("FLOAT")) {
            setValue(updateStmt, j + 1, vals[j + 1], Types.FLOAT);
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
        if (value.startsWith("'") && value.endsWith("'")) { // 处理空字符串'', 非空字符串包含特殊字符的情况'""'
          stmt.setString(index, value.substring(1, value.length() - 1));
        } else {
          stmt.setString(index, value);
        }
    }
  }

  private String getTableNameByDB(String databaseName, String name) {
    return getQuotName(databaseName) + SEPARATOR + getQuotName(name);
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    if (param.getType() == Expression.ExpressionType.Base) {
      return "%s(CAST(%s AS DECIMAL(34, 16)))";
    }
    return "%s(%s)";
  }

  public Map<String, ColumnField> getColumnMap(
      Connection conn, String databaseName, String tableName) {
    List<ColumnField> columnFieldList = getColumns(conn, databaseName, tableName, null);
    return columnFieldList.stream()
        .collect(Collectors.toMap(ColumnField::getColumnName, field -> field));
  }

  private List<ColumnField> getColumns(
      Connection conn, String databaseName, String tableName, String columnNamePattern) {
    try {
      DatabaseMetaData databaseMetaData = conn.getMetaData();
      ResultSet rs =
          databaseMetaData.getColumns(
              databaseName, getSchemaPattern(databaseName), tableName, columnNamePattern);
      List<ColumnField> columnFields = new ArrayList<>();
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        String columnType = rs.getString("TYPE_NAME");
        String columnTable = rs.getString("TABLE_NAME");
        int columnSize = rs.getInt("COLUMN_SIZE");
        columnFields.add(new ColumnField(columnTable, columnName, columnType, columnSize));
      }
      rs.close();
      conn.close();
      return columnFields;
    } catch (SQLException e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
    }
  }
}
