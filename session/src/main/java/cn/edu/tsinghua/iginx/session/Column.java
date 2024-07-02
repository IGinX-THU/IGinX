package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Column {

  private final String path;

  private final DataType dataType;

  public Column(String path, DataType dataType) {
    this.path = path;
    this.dataType = dataType;
  }

  public String getPath() {
    return path;
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    return "Column{" + "path='" + path + '\'' + ", dataType=" + dataType + '}';
  }
}
