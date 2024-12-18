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
package cn.edu.tsinghua.iginx.vectordb.tools;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;

public class VectorDBSchema {

  private final String databaseName;

  private final String collectionName;

  private final String fieldName;

  public String getCollectionName() {
    return collectionName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public char getQuote() {
    return quote;
  }

  private final char quote;

  public VectorDBSchema(String path, char quote) {
    this(path, false, quote);
  }

  public VectorDBSchema(String path, boolean isDummy, char quote) {
    int firstSeparator = path.indexOf(".");
    if (isDummy) {
      databaseName = path.substring(0, firstSeparator);
      path = path.substring(firstSeparator + 1);
    } else {
      databaseName = "";
    }
    int lastSeparator = path.lastIndexOf(".");
    collectionName = path.substring(0, lastSeparator);
    fieldName = path.substring(lastSeparator + 1);
    this.quote = quote;
  }

  public static String getQuoteFullName(String tableName, String columnName, char quote) {
    return getQuotName(tableName, quote) + SEPARATOR + getQuotName(columnName, quote);
  }

  public static String getFullName(String tableName, String columnName) {
    return tableName + SEPARATOR + columnName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getQuoteFullName() {
    return getQuotName(collectionName, quote) + SEPARATOR + getQuotName(fieldName, quote);
  }

  public String getQuotCollectionName() {
    return getQuotName(collectionName, quote);
  }

  public String getQuotFieldName() {
    return getQuotName(fieldName, quote);
  }

  private static String getQuotName(String name, char quote) {
    return quote + name + quote;
  }
}
