package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

public class FileMeta {
  private byte[] magicNumber;
  private DataType dataType;
  private Map<String, String> tags = new HashMap<>();

  public FileMeta() {
    this.dataType = null;
  }

  public FileMeta(DataType dataType, Map<String, String> tag) {
    this.dataType = dataType;
    this.tags = tag;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public byte[] getMagicNumber() {
    return magicNumber;
  }

  public void setMagicNumber(byte[] magicNumber) {
    this.magicNumber = magicNumber;
  }
}
