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

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class DefaultDatabaseStrategy implements DatabaseStrategy {

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  @Override
  public String getUrl(String databaseName, StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String engine = extraParams.get("engine");
    return String.format("jdbc:%s://%s:%s/%s", engine, meta.getIp(), meta.getPort(), databaseName);
  }

  @Override
  public String getConnectUrl(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);
    String engine = extraParams.get("engine");

    return password == null
        ? String.format("jdbc:%s://%s:%s/?user=%s", engine, meta.getIp(), meta.getPort(), username)
        : String.format(
            "jdbc:%s://%s:%s/?user=%s&password=%s",
            engine, meta.getIp(), meta.getPort(), username, password);
  }

  @Override
  public String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException {
    return rs.getString("DATNAME");
  }

  @Override
  public String getSchemaPattern(String databaseName, AbstractRelationalMeta relationalMeta) {
    return relationalMeta.getSchemaPattern();
  }

  @Override
  public String formatConcatStatement(List<String> columns) {
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
    // 默认的批量插入实现
    for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
      String tableName = entry.getKey();
      String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
      List<String> values = entry.getValue().v;

      StringBuilder statement = new StringBuilder();
      statement.append("INSERT INTO ");
      statement.append(quote + tableName + quote);
      statement.append(" (");
      statement.append(quote + "key" + quote);
      statement.append(", ");

      // 处理列名
      String[] parts = columnNames.split(", ");
      StringBuilder columnNamesBuilder = new StringBuilder();
      for (String part : parts) {
        columnNamesBuilder.append(quote + part + quote).append(", ");
      }
      statement.append(columnNamesBuilder.substring(0, columnNamesBuilder.length() - 2));

      statement.append(") VALUES ");
      for (String value : values) {
        statement.append("(");
        statement.append(value, 0, value.length() - 2);
        statement.append("), ");
      }
      statement.delete(statement.length() - 2, statement.length());

      // 处理冲突
      statement.append(" ON CONFLICT (").append(quote + "key" + quote).append(") DO UPDATE SET ");
      for (String part : parts) {
        if (part.equals("key")) continue;
        statement
            .append(quote + part + quote)
            .append(" = EXCLUDED.")
            .append(quote + part + quote)
            .append(", ");
      }
      statement.delete(statement.length() - 2, statement.length());
      statement.append(";");

      stmt.addBatch(statement.toString());
    }
    stmt.executeBatch();
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    return "%s(%s)";
  }

  @Override
  public boolean needSpecialBatchInsert() {
    return false;
  }

  @Override
  public void batchInsert(
      Connection conn,
      String tableName,
      Map<String, ColumnField> columnMap,
      String[] parts,
      List<String> values)
      throws SQLException {
    // 默认实现为空，子类需要覆盖此方法
    throw new UnsupportedOperationException("This database does not support special batch insert");
  }
}
