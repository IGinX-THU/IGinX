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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDatabaseStrategy extends AbstractDatabaseStrategyWithoutUpsert {
  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseStrategy.class);

  public OracleDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
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
    String database = extraParams.getOrDefault(DATABASE, relationalMeta.getDefaultDatabaseName());

    return String.format(
        "jdbc:oracle:thin:\"%s\"/\"%s\"@%s:%d/%s",
        username, password, storageEngineMeta.getIp(), storageEngineMeta.getPort(), database);
  }

  @Override
  public void configureDataSource(
      HikariConfig config, String databaseName, StorageEngineMeta meta) {
    config.setUsername(null);
    config.setPassword(null);
    if (!databaseName.isEmpty()) {
      config.setConnectionInitSql(
          "ALTER SESSION SET CURRENT_SCHEMA = " + getQuotName(databaseName));
    }
  }

  @Override
  public Map<String, ColumnField> getColumnMap(
      Connection conn, String databaseName, String tableName) throws SQLException {
    String fullTableName = databaseName + DOT + tableName;
    List<ColumnField> columnFieldList =
        getColumns(conn, getSchemaPattern(null, false), fullTableName);
    return columnFieldList.stream()
        .collect(Collectors.toMap(ColumnField::getColumnName, field -> field));
  }
}
