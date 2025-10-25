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
package cn.edu.tsinghua.iginx.neo4j.tools;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;

public class Neo4jSchema {

  private final String databaseName;

  private final String labelName;

  private final String propertyName;

  private static final char quote = '`';

  public Neo4jSchema(String path) {
    this(path, false);
  }

  public Neo4jSchema(String path, boolean isDummy) {
    databaseName = "";
    int lastSeparator = path.lastIndexOf(".");
    labelName = path.substring(0, lastSeparator);
    propertyName = path.substring(lastSeparator + 1);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getLabelName() {
    return labelName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  private static String getQuoteName(String name, char quote) {
    return quote + name + quote;
  }

  public static String getQuoteName(String name) {
    name = name.replaceFirst("^" + quote, "").replaceFirst(quote + "$", "");
    return quote + name + quote;
  }

  public static String getQuoteFullName(String labelName, String propertyName) {
    return getQuoteName(labelName) + DOT + getQuoteName(propertyName);
  }

  public String getFullName() {
    return getQuoteFullName(labelName, propertyName);
  }

  public static String getFullName(String tableName, String columnName) {
    return tableName + DOT + columnName;
  }
}
