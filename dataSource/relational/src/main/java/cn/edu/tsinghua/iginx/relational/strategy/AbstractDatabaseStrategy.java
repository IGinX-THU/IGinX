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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.exception.RelationalException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDatabaseStrategy implements DatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDatabaseStrategy.class);

  protected final AbstractRelationalMeta relationalMeta;

  protected final StorageEngineMeta storageEngineMeta;

  protected final int boundaryLevel;

  private final Map<String, HikariDataSource> connectionPoolMap = new ConcurrentHashMap<>();

  private Connection connection;

  public AbstractDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    this.relationalMeta = relationalMeta;
    this.storageEngineMeta = storageEngineMeta;
    this.boundaryLevel =
        Integer.parseInt(storageEngineMeta.getExtraParams().getOrDefault(BOUNDARY_LEVEL, "0"));
  }

  @Override
  public Connection initConnection() throws StorageInitializationException {
    try {
      connection = DriverManager.getConnection(getConnectUrl());
    } catch (SQLException e) {
      throw new StorageInitializationException(
          String.format("cannot connect to %s :", storageEngineMeta), e);
    }
    return connection;
  }

  @Override
  public Connection getConnection(String databaseName) {
    if (databaseName.startsWith("dummy")) {
      return null;
    }

    if (relationalMeta.getSystemDatabaseName().stream().anyMatch(databaseName::equalsIgnoreCase)) {
      return null;
    }

    if (relationalMeta.isSupportCreateDatabase()) {
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
      config.setJdbcUrl(getUrl(databaseName));
      config.setUsername(storageEngineMeta.getExtraParams().get(USERNAME));
      config.setPassword(storageEngineMeta.getExtraParams().get(PASSWORD));
      config.addDataSourceProperty(
          "prepStmtCacheSize",
          storageEngineMeta.getExtraParams().getOrDefault("prep_stmt_cache_size", "250"));
      config.setLeakDetectionThreshold(
          Long.parseLong(
              storageEngineMeta.getExtraParams().getOrDefault("leak_detection_threshold", "2500")));
      config.setConnectionTimeout(
          Long.parseLong(
              storageEngineMeta.getExtraParams().getOrDefault("connection_timeout", "30000")));
      config.setIdleTimeout(
          Long.parseLong(storageEngineMeta.getExtraParams().getOrDefault("idle_timeout", "10000")));
      config.setMaximumPoolSize(
          Integer.parseInt(
              storageEngineMeta.getExtraParams().getOrDefault("maximum_pool_size", "20")));
      config.setMinimumIdle(
          Integer.parseInt(storageEngineMeta.getExtraParams().getOrDefault("minimum_idle", "1")));
      config.addDataSourceProperty(
          "prepStmtCacheSqlLimit",
          storageEngineMeta.getExtraParams().getOrDefault("prep_stmt_cache_sql_limit", "2048"));
      configureDataSource(config, databaseName, storageEngineMeta);

      HikariDataSource newDataSource = new HikariDataSource(config);
      connectionPoolMap.put(databaseName, newDataSource);
      return newDataSource.getConnection();
    } catch (SQLException | HikariPool.PoolInitializationException e) {
      LOGGER.error("Cannot get connection for database {}", databaseName, e);
      return null;
    }
  }

  @Override
  public void closeConnection(String databaseName) {
    HikariDataSource dataSource = connectionPoolMap.get(databaseName);
    if (dataSource != null) {
      dataSource.close();
      connectionPoolMap.remove(databaseName);
    }
  }

  @Override
  public void release() throws PhysicalException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RelationalException(e);
    }
  }

  @Override
  public String getUrl(String databaseName) {
    Map<String, String> extraParams = storageEngineMeta.getExtraParams();
    String engine = extraParams.get("engine");
    return String.format(
        "jdbc:%s://%s:%s/%s",
        engine, storageEngineMeta.getIp(), storageEngineMeta.getPort(), databaseName);
  }

  @Override
  public String getConnectUrl() {
    Map<String, String> extraParams = storageEngineMeta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);
    String engine = extraParams.get("engine");

    return password == null
        ? String.format(
            "jdbc:%s://%s:%s/?user=%s",
            engine, storageEngineMeta.getIp(), storageEngineMeta.getPort(), username)
        : String.format(
            "jdbc:%s://%s:%s/?user=%s&password=%s",
            engine, storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password);
  }

  @Override
  public void configureDataSource(
      HikariConfig config, String databaseName, StorageEngineMeta meta) {}

  @Override
  public String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException {
    return rs.getString("DATNAME");
  }

  @Override
  public String getDatabasePattern(String databaseName, boolean isDummy) {
    return databaseName;
  }

  @Override
  public String getSchemaPattern(String databaseName, boolean isDummy) {
    return relationalMeta.getSchemaPattern();
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
    conn.setAutoCommit(false);
    // 默认的批量插入实现
    for (Map.Entry<String, Pair<String, List<String>>> entry : tableToColumnEntries.entrySet()) {
      String tableName = entry.getKey();
      String columnNames = entry.getValue().k.substring(0, entry.getValue().k.length() - 2);
      List<String> values = entry.getValue().v;

      // 处理列名
      String[] parts = columnNames.split(", ");
      StringBuilder statement = new StringBuilder();
      statement.append("INSERT INTO ").append(getQuotName(tableName)).append(" (");
      statement.append(getQuotName(KEY_NAME)).append(", ");

      for (int i = 0; i < parts.length; i++) {
        statement.append(getQuotName(parts[i]));
        if (i < parts.length - 1) statement.append(", ");
      }
      statement.append(") VALUES (");

      // 占位符 ?
      for (int i = 0; i < parts.length + 1; i++) { // +1 因为还有 KEY_NAME
        statement.append("?");
        if (i < parts.length) statement.append(", ");
      }
      statement.append(") ");

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

      // 准备语句
      try (PreparedStatement ps = conn.prepareStatement(statement.toString())) {
        int batchSize = 0;
        for (String value : values) {
          // 这里你原来是 "(1, 'abc', 'def', )" 这种拼出来的字符串
          // 建议改为 List<Object> 保存，然后逐列 setObject
          String[] tokens = value.substring(0, value.length() - 2).split(", ");
          for (int i = 0; i < tokens.length; i++) {
            ps.setObject(i + 1, tokens[i]); // JDBC 会自动转义
          }
          ps.addBatch();

          if (++batchSize % 20 == 0) { // 控制 batch 大小
            ps.executeBatch();
            batchSize = 0;
          }
        }
        if (batchSize > 0) {
          ps.executeBatch();
        }
      }
    }
    // 最后统一提交
    conn.commit();
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    return "%s(%s)";
  }

  @Override
  public String getQuotName(String name) {
    return String.format("%s%s%s", relationalMeta.getQuote(), name, relationalMeta.getQuote());
  }
}
