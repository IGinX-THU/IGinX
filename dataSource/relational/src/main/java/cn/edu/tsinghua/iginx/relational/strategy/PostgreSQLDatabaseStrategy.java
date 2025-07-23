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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;

public class PostgreSQLDatabaseStrategy extends AbstractDatabaseStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLDatabaseStrategy.class);

  public PostgreSQLDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
  }

  @Override
  public ColumnsInterval getColumnsBoundary()
          throws PhysicalException, SQLException {
    StringBuilder sqlGetDBBuilder = new StringBuilder();
    sqlGetDBBuilder.append("SELECT min(datname), max(datname)");
    sqlGetDBBuilder
            .append(" FROM ( ")
            .append(
                    relationalMeta.getDatabaseQuerySql(),
                    0,
                    relationalMeta.getDatabaseQuerySql().length() - 1)
            .append(" ) datnames");
    sqlGetDBBuilder
            .append(" WHERE datname NOT IN ('")
            .append(relationalMeta.getDefaultDatabaseName())
            .append("'");
    for (String systemDatabaseName : relationalMeta.getSystemDatabaseName()) {
      sqlGetDBBuilder.append(", '").append(systemDatabaseName).append("'");
    }
    sqlGetDBBuilder.append(")");

    String sqlGetDB = sqlGetDBBuilder.toString();

    LOGGER.debug("[Query] execute query: {}", sqlGetDB);

    try (Connection conn = getConnection(relationalMeta.getDefaultDatabaseName());
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery(sqlGetDB)) {
      if (rs.next()) {
        String minDatabaseName = rs.getString(1);
        String maxDatabaseName = rs.getString(2);
        if (minDatabaseName != null && maxDatabaseName != null) {
          return getBoundaryFromInformationSchemaInCatalog(minDatabaseName, maxDatabaseName);
        }
      }
      throw new RelationalTaskExecuteFailureException("no data!");
    }
  }

  public ColumnsInterval getBoundaryFromInformationSchemaInCatalog(String minDb, String maxDb)
          throws SQLException {
    if (relationalMeta.isUseApproximateBoundary()) {
      return new ColumnsInterval(minDb, StringUtils.nextString(maxDb));
    }
    String columnNames = "table_catalog, table_name, column_name";
    String conditionStatement = " WHERE table_schema LIKE '" + relationalMeta.getSchemaPattern() + "'";
    String sqlMin =
            "SELECT "
                    + columnNames
                    + " FROM information_schema.columns"
                    + conditionStatement
                    + " ORDER BY table_catalog, table_name, column_name LIMIT 1";
    String sqlMax =
            "SELECT "
                    + columnNames
                    + " FROM information_schema.columns"
                    + conditionStatement
                    + " ORDER BY table_catalog DESC, table_name DESC, column_name DESC LIMIT 1";

    String minPath = null;
    try (Connection conn = getConnection(minDb);
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery(sqlMin)) {
      if (rs.next()) {
        minPath = rs.getString(1) + SEPARATOR + rs.getString(2) + SEPARATOR + rs.getString(3);
      }
    }
    String maxPath = null;
    try (Connection conn = getConnection(maxDb);
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery(sqlMax)) {
      if (rs.next()) {
        maxPath = rs.getString(1) + SEPARATOR + rs.getString(2) + SEPARATOR + rs.getString(3);
      }
    }
    minPath = minPath == null ? minDb : minPath;
    maxPath = maxPath == null ? maxDb : maxPath;
    return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  }
}
