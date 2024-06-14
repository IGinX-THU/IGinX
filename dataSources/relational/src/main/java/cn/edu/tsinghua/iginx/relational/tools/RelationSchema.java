package cn.edu.tsinghua.iginx.relational.tools;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;

public class RelationSchema {

  private final String databaseName;

  private final String tableName;

  private final String columnName;

  private final char quote;

  public RelationSchema(String path, char quote) {
    this(path, false, quote);
  }

  public RelationSchema(String path, boolean isDummy, char quote) {
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

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getQuoteFullName() {
    return getQuotName(tableName, quote) + SEPARATOR + getQuotName(columnName, quote);
  }

  public String getQuotTableName() {
    return getQuotName(tableName, quote);
  }

  public String getQuotColumnName() {
    return getQuotName(columnName, quote);
  }

  private static String getQuotName(String name, char quote) {
    return quote + name + quote;
  }
}
