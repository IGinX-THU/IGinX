package cn.edu.tsinghua.iginx.relational.tools;

public class ColumnField {
  public String tableName;
  public String columnName;
  public String columnType;

  public ColumnField(String tableName, String columnName, String columnType) {
    this.tableName = tableName;
    this.columnName = columnName;
    this.columnType = columnType;
  }
}
