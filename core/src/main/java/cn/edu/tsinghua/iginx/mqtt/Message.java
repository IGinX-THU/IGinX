package cn.edu.tsinghua.iginx.mqtt;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Message {

  private String path;

  private long key;

  private DataType dataType;

  private Object value;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getKey() {
    return key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
