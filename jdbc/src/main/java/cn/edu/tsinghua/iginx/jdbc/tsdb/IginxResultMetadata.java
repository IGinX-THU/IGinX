/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.jdbc.tsdb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

public class IginxResultMetadata implements ResultSetMetaData {

  private final List<String> columnNames;
  private final List<Integer> columnTypes;

  public IginxResultMetadata(List<String> columnNames, List<Integer> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  @Override
  public int getColumnCount() {
    return columnNames.size();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    checkColumnIndex(column);
    return columnNames.get(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    checkColumnIndex(column);
    return columnTypes.get(column - 1);
  }

  private void checkColumnIndex(int column) throws SQLException {
    if (columnNames == null || columnNames.isEmpty()) throw new SQLException("No column exists");
    if (column < 1) throw new SQLException("Column Index out of range, " + column + " < 1");
    if (column > columnNames.size())
      throw new SQLException("Column Index out of range, " + column + " > " + columnNames.size());
  }

  @Override
  public String getColumnTypeName(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isSearchable(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCurrency(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int isNullable(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isSigned(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchemaName(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getPrecision(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getScale(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getTableName(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogName(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWritable(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getColumnClassName(int column) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
