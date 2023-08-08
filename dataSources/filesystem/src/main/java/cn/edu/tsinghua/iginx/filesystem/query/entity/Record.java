package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Record {
  private static final Logger logger = LoggerFactory.getLogger(Record.class);
  private long key;
  private DataType dataType;
  private Object rawData;

  public Record(long key, DataType dataType, Object rawData) {
    this.key = key;
    this.dataType = dataType;
    this.rawData = rawData;
  }

  public Record(long key, Object rawData) {
    this.key = key;
    this.rawData = rawData;
    this.dataType = DataType.BINARY;
  }

  public Object getRawData() {
    return rawData;
  }

  public long getKey() {
    return key;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public void setRawData(Object rawData) {
    this.rawData = rawData;
  }
}
