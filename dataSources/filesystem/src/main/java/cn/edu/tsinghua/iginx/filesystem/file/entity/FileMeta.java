package cn.edu.tsinghua.iginx.filesystem.file.entity;

import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.MAGIC_NUMBER;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

public class FileMeta {

  private byte[] magicNumber = MAGIC_NUMBER;

  private DataType dataType;

  private Map<String, String> tags = new HashMap<>();

  public FileMeta() {}

  public FileMeta(DataType dataType, Map<String, String> tags) {
    this.dataType = dataType;
    if (tags != null) {
      this.tags = tags;
    }
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
