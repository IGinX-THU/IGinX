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

public interface DatabaseStrategy {
  String getQuotName(String name);

  String getUrl(String databaseName, StorageEngineMeta meta);

  String getConnectUrl(StorageEngineMeta meta);

  String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException;

  String getSchemaPattern(String databaseName, AbstractRelationalMeta relationalMeta);

  String formatConcatStatement(List<String> columns);

  void executeBatchInsert(
      Connection conn,
      String databaseName,
      Statement stmt,
      Map<String, Pair<String, List<String>>> tableToColumnEntries,
      char quote)
      throws SQLException;

  String getAvgCastExpression(Expression param);

  boolean needSpecialBatchInsert();

  void batchInsert(
      Connection conn,
      String tableName,
      Map<String, ColumnField> columnMap,
      String[] parts,
      List<String> values)
      throws SQLException;
}
