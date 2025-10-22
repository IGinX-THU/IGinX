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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.exception.RelationalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLDatabaseStrategy extends AbstractDatabaseStrategy {
  MySQLDatabaseStrategy(
      AbstractRelationalMeta relationalMeta, StorageEngineMeta storageEngineMeta) {
    super(relationalMeta, storageEngineMeta);
  }

  @Override
  public String getAvgCastExpression(Expression param) {
    if (param.getType() == Expression.ExpressionType.Base) {
      return "%s(CAST(%s AS DECIMAL(34, 16)))";
    }
    return super.getAvgCastExpression(param);
  }

  @Override
  public ColumnsInterval getColumnsBoundary() throws PhysicalException, SQLException {
    String defaultDb = relationalMeta.getDefaultDatabaseName();
    String columnNames = "table_schema, table_name, column_name";
    List<String> exceptSchema = new ArrayList<>();
    exceptSchema.add(relationalMeta.getDefaultDatabaseName());
    exceptSchema.addAll(relationalMeta.getSystemDatabaseName());
    String conditionStatement =
        exceptSchema.stream()
            .map(s -> "'" + s + "'")
            .collect(Collectors.joining(", ", " WHERE table_schema NOT IN (", ")"));
    if (boundaryLevel < 1) {
      String sql =
          "SELECT min(table_schema), max(table_schema) FROM information_schema.tables "
              + conditionStatement;
      try (Connection conn = getConnection(defaultDb);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        if (rs.next()) {
          String minPath = rs.getString(1);
          String maxPath = rs.getString(2);
          return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
        } else {
          throw new RelationalTaskExecuteFailureException("no data!");
        }
      }
    }
    String sqlMin =
        "SELECT "
            + columnNames
            + " FROM information_schema.columns"
            + conditionStatement
            + " ORDER BY table_schema, table_name, column_name LIMIT 1";
    String sqlMax =
        "SELECT "
            + columnNames
            + " FROM information_schema.columns"
            + conditionStatement
            + " ORDER BY table_schema DESC, table_name DESC, column_name DESC LIMIT 1";

    String minPath = null;
    String maxPath = null;
    try (Connection conn = getConnection(defaultDb);
        Statement statement = conn.createStatement()) {
      try (ResultSet rs = statement.executeQuery(sqlMin)) {
        if (rs.next()) {
          minPath = rs.getString(1) + DOT + rs.getString(2) + DOT + rs.getString(3);
        }
      }
      try (ResultSet rs = statement.executeQuery(sqlMax)) {
        if (rs.next()) {
          maxPath = rs.getString(1) + DOT + rs.getString(2) + DOT + rs.getString(3);
        }
      }
    }
    if (minPath == null || maxPath == null) {
      throw new RelationalTaskExecuteFailureException("no data!");
    }
    return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  }
}
