package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Point {

  private final String path;

  private final DataType dataType;

  private final long timestamp;

  private final Object value;

  public Point(String path, DataType dataType, long timestamp, Object value) {
    this.path = path;
    this.dataType = dataType;
    this.timestamp = timestamp;
    this.value = value;
  }

  public String getPath() {
    return path;
  }

  public DataType getDataType() {
    return dataType;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getValue() {
    return value;
  }
}
