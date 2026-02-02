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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.strategy.base.AbstractDatabaseStrategyWithoutUpsert;
import cn.edu.tsinghua.iginx.relational.tools.ColumnField;
import com.zaxxer.hikari.HikariConfig;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengDatabaseStrategy extends AbstractDatabaseStrategyWithoutUpsert {
  private static final Logger LOGGER = LoggerFactory.getLogger(DamengDatabaseStrategy.class);

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
  }

  @Override
  public String getUrl(String databaseName) {
    return getConnectUrl();
  }

  @Override
  public String getConnectUrl() {
    Map<String, String> extraParams = storageEngineMeta.getExtraParams();
    String username = extraParams.get(USERNAME);
    String password = extraParams.get(PASSWORD);

    return String.format(
        "jdbc:dm://%s:%s?user=%s&password=\"%s\"",
        storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password);
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
  public Map<String, ColumnField> getColumnMap(
      Connection conn, String databaseName, String tableName) throws SQLException {
    List<ColumnField> columnFieldList;
    if (relationalMeta.isSupportCreateDatabase()) {
      columnFieldList = getColumns(conn, getSchemaPattern(databaseName, false), tableName);
    } else {
      columnFieldList =
          getColumns(conn, getSchemaPattern(null, false), databaseName + DOT + tableName);
    }
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
}
