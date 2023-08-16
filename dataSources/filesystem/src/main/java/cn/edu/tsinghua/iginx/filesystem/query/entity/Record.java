package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class Record {
  private long key;
  private Value value;

  public Record(long key, DataType dataType, Object rawData) {
    this.key = key;
    this.value = new Value(dataType, rawData);
  }

  public Record(long key, Object rawData) {
    this.key = key;
    this.value = new Value(rawData);
  }

  public Object getRawData() {
    return value.getValue();
  }

  public long getKey() {
    return key;
  }

  public DataType getDataType() {
    return value.getDataType();
  }

  public void setKey(long key) {
    this.key = key;
  }
}
