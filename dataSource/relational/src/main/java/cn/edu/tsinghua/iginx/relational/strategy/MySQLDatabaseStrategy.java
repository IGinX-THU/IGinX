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
  //
  //  @Override
  //  public ColumnsInterval getBoundaryFromInformationSchema()
  //          throws PhysicalException, SQLException {
  //    return getBoundaryFromInformationSchemaInCatalog(
  //            relationalMeta.getDefaultDatabaseName(), relationalMeta.getDefaultDatabaseName());
  //  }
  //
  //  @Override
  //  public ColumnsInterval getBoundaryFromInformationSchemaInCatalog(String minDb, String maxDb)
  //          throws SQLException, RelationalTaskExecuteFailureException {
  //    String columnNames;
  //    String conditionStatement;
  //    columnNames = "table_schema, table_name, column_name";
  //    List<String> exceptSchema = new ArrayList<>();
  //    exceptSchema.add(relationalMeta.getDefaultDatabaseName());
  //    exceptSchema.addAll(relationalMeta.getSystemDatabaseName());
  //    conditionStatement =
  //            exceptSchema.stream()
  //                    .map(s -> "'" + s + "'")
  //                    .collect(Collectors.joining(", ", " WHERE table_schema NOT IN (", ")"));
  //    if (relationalMeta.isUseApproximateBoundary()) {
  //      String sql =
  //              "SELECT min(table_schema), max(table_schema) FROM information_schema.tables "
  //                      + conditionStatement;
  //      try (Connection conn = getConnection(minDb);
  //           Statement statement = conn.createStatement();
  //           ResultSet rs = statement.executeQuery(sql)) {
  //        if (rs.next()) {
  //          String minPath = rs.getString(1);
  //          String maxPath = rs.getString(2);
  //          return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  //        } else {
  //          throw new RelationalTaskExecuteFailureException("no data!");
  //        }
  //      }
  //    }
  //    String sqlMin =
  //            "SELECT "
  //                    + columnNames
  //                    + " FROM information_schema.columns"
  //                    + conditionStatement
  //                    + " ORDER BY table_catalog, table_name, column_name LIMIT 1";
  //    String sqlMax =
  //            "SELECT "
  //                    + columnNames
  //                    + " FROM information_schema.columns"
  //                    + conditionStatement
  //                    + " ORDER BY table_catalog DESC, table_name DESC, column_name DESC LIMIT 1";
  //
  //    String minPath = null;
  //    try (Connection conn = getConnection(minDb);
  //         Statement statement = conn.createStatement();
  //         ResultSet rs = statement.executeQuery(sqlMin)) {
  //      if (rs.next()) {
  //        minPath = rs.getString(1) + SEPARATOR + rs.getString(2) + SEPARATOR + rs.getString(3);
  //      }
  //    }
  //    String maxPath = null;
  //    try (Connection conn = getConnection(maxDb);
  //         Statement statement = conn.createStatement();
  //         ResultSet rs = statement.executeQuery(sqlMax)) {
  //      if (rs.next()) {
  //        maxPath = rs.getString(1) + SEPARATOR + rs.getString(2) + SEPARATOR + rs.getString(3);
  //      }
  //    }
  //    if (minPath == null || maxPath == null) {
  //      throw new RelationalTaskExecuteFailureException("no data!");
  //    }
  //    return new ColumnsInterval(minPath, StringUtils.nextString(maxPath));
  //  }
}
