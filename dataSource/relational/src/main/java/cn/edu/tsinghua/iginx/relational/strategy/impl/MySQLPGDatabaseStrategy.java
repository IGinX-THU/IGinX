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
package cn.edu.tsinghua.iginx.relational.strategy.impl;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.strategy.base.AbstractDatabaseStrategy;
import cn.edu.tsinghua.iginx.relational.tools.SqlStringUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLPGDatabaseStrategy extends AbstractDatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLPGDatabaseStrategy.class);

  public MySQLPGDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
  }

  @Override
  public String getQuotName(String name) {
    return super.getQuotName(name);
  }

  private static String stripTrailingSemicolon(String sql) {
    if (sql == null) {
      return null;
    }
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private String buildExceptDbsSqlList() {
    List<String> except = new ArrayList<>();
    except.add(relationalMeta.getDefaultDatabaseName());
    except.addAll(relationalMeta.getSystemDatabaseName());
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < except.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("'").append(SqlStringUtils.escapeSqlQuotedContent(except.get(i), '\'')).append("'");
    }
    return sb.toString();
  }

  private static String defaultIfEmpty(String s, String defaultValue) {
    return (s == null || s.isEmpty()) ? defaultValue : s;
  }

  @Override
  public ColumnsInterval getColumnsBoundary() throws PhysicalException, SQLException {
    if (!relationalMeta.isSupportBoundaryQuery()) {
      throw new RelationalTaskExecuteFailureException("boundary query is not supported");
    }

    String defaultDb = relationalMeta.getDefaultDatabaseName();

    if (boundaryLevel < 1) {
      String baseSql = stripTrailingSemicolon(relationalMeta.getDatabaseQuerySql());
      String sqlGetDB =
          "SELECT min(datname), max(datname) FROM ("
              + baseSql
              + ") datnames WHERE datname NOT IN ("
              + buildExceptDbsSqlList()
              + ")";
      try (Connection conn = getConnection(defaultDb);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery(sqlGetDB)) {
        if (rs.next()) {
          String minDb = rs.getString(1);
          String maxDb = rs.getString(2);
          if (minDb != null && maxDb != null) {
            return new ColumnsInterval(minDb, StringUtils.nextString(maxDb));
          }
        }
      }
      throw new RelationalTaskExecuteFailureException("no data!");
    }

    String minDb = defaultDb;
    String maxDb = defaultDb;
    if (relationalMeta.isBoundaryQueryUseDbConnection()) {
      String baseSql = stripTrailingSemicolon(relationalMeta.getDatabaseQuerySql());
      String sqlGetDB =
          "SELECT min(datname), max(datname) FROM ("
              + baseSql
              + ") datnames WHERE datname NOT IN ("
              + buildExceptDbsSqlList()
              + ")";
      try (Connection conn = getConnection(defaultDb);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery(sqlGetDB)) {
        if (rs.next()) {
          minDb = rs.getString(1);
          maxDb = rs.getString(2);
        }
      }
      if (minDb == null || maxDb == null) {
        throw new RelationalTaskExecuteFailureException("no data!");
      }
    }

    String sqlMin;
    String sqlMax;
    String catalogCol =
        defaultIfEmpty(relationalMeta.getBoundaryQueryCatalogColumn(), "table_schema");
    String schemaCol =
        defaultIfEmpty(relationalMeta.getBoundaryQuerySchemaColumn(), "table_schema");

    List<String> conditions = new ArrayList<>();
    if (!relationalMeta.isBoundaryQueryUseDbConnection()) {
      conditions.add(schemaCol + " NOT IN (" + buildExceptDbsSqlList() + ")");
    }
    String schemaPattern = relationalMeta.getSchemaPattern();
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      conditions.add(
          schemaCol + " LIKE '" + SqlStringUtils.escapeSqlQuotedContent(schemaPattern, '\'') + "'");
    }
    String where = conditions.isEmpty() ? "" : (" WHERE " + String.join(" AND ", conditions));

    String columnNames = catalogCol + ", table_name, column_name";
    sqlMin =
        "SELECT "
            + columnNames
            + " FROM information_schema.columns"
            + where
            + " ORDER BY "
            + catalogCol
            + ", table_name, column_name LIMIT 1";
    sqlMax =
        "SELECT "
            + columnNames
            + " FROM information_schema.columns"
            + where
            + " ORDER BY "
            + catalogCol
            + " DESC, table_name DESC, column_name DESC LIMIT 1";

    String minPath = null;
    try (Connection conn = getConnection(minDb);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sqlMin)) {
      if (rs.next()) {
        ResultSetMetaData meta = rs.getMetaData();
        int cnt = meta.getColumnCount();
        if (cnt >= 3) {
          minPath =
              rs.getString(1)
                  + GlobalConstant.DOT
                  + rs.getString(2)
                  + GlobalConstant.DOT
                  + rs.getString(3);
        } else {
          minPath = rs.getString(1);
        }
      }
    }

    String maxPath = null;
    try (Connection conn = getConnection(maxDb);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sqlMax)) {
      if (rs.next()) {
        ResultSetMetaData meta = rs.getMetaData();
        int cnt = meta.getColumnCount();
        if (cnt >= 3) {
          maxPath =
              rs.getString(1)
                  + GlobalConstant.DOT
                  + rs.getString(2)
                  + GlobalConstant.DOT
                  + rs.getString(3);
        } else {
          maxPath = rs.getString(1);
        }
      }
    }

    minPath = minPath == null ? minDb : minPath;
    maxPath = maxPath == null ? maxDb : maxPath;
    return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  }

  @Override
  public String formatConcatStatement(List<String> columns) {
    return super.formatConcatStatement(columns);
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

      StringBuilder statement = new StringBuilder();
      statement.append("INSERT INTO ");
      statement.append(getQuotName(tableName));
      statement.append(" (");
      statement.append(getQuotName(KEY_NAME));
      statement.append(", ");

      // 处理列名
      String[] parts = columnNames.split(", ");
      StringBuilder columnNamesBuilder = new StringBuilder();
      for (String part : parts) {
        columnNamesBuilder.append(getQuotName(part)).append(", ");
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
      statement.append(relationalMeta.getUpsertStatement());
      for (String part : parts) {
        if (part.equals(KEY_NAME)) continue;
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
}
