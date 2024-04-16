package cn.edu.tsinghua.iginx.RelationAbstract.tools;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;

public class RelationSchema {

  private final String databaseName;

  private final String tableName;

  private final String columnName;

  public RelationSchema(String path) {
    this(path, false);
  }

  public RelationSchema(String path, boolean isDummy) {
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

  public RelationSchema(String tableName, String columnName) {
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
