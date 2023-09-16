package cn.edu.tsinghua.iginx.postgresql.tools;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;

public class PostgreSQLSchema {

  private final String databaseName;

  private final String tableName;

  private final String columnName;

  public PostgreSQLSchema(String path) {
    this(path, false);
  }

  public PostgreSQLSchema(String path, boolean isDummy) {
    int firstSeparator = path.indexOf(".");
    if (isDummy) {
      databaseName = path.substring(0, firstSeparator);
      path = path.substring(firstSeparator + 1);
    } else {
      databaseName = "";
    }
    int lastSeparator = path.lastIndexOf(".");
    tableName = path.substring(0, lastSeparator);
    columnName = path.substring(lastSeparator + 1);
  }

  public PostgreSQLSchema(String tableName, String columnName) {
    this.databaseName = "";
    this.tableName = tableName;
    this.columnName = columnName;
  }

  public static String getQuotFullName(String tableName, String columnName) {
    return getQuotName(tableName) + SEPARATOR + getQuotName(columnName);
  }

  public static String getFullName(String tableName, String columnName) {
    return tableName + SEPARATOR + columnName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getQuotFullName() {
    return getQuotName(tableName) + SEPARATOR + getQuotName(columnName);
  }

  public String getQuotTableName() {
    return getQuotName(tableName);
  }

  public String getQuotColumnName() {
    return getQuotName(columnName);
  }

  private static String getQuotName(String name) {
    return "\"" + name + "\"";
  }
}
