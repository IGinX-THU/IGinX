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
package cn.edu.tsinghua.iginx.relational.tools;

public class ColumnField {
  public String tableName;
  public String columnName;
  public String columnType;
  public int columnSize; // for oracle

  public ColumnField(String tableName, String columnName, String columnType) {
    this.tableName = tableName;
    this.columnName = columnName;
    this.columnType = columnType;
  }

  public ColumnField(String tableName, String columnName, String columnType, int columnSize) {
    this.tableName = tableName;
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnSize = columnSize;
  }

  public String getColumnName() {
    return columnName;
  }
}
