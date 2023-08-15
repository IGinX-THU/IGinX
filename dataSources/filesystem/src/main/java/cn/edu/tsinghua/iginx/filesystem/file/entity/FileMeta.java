package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;

public class FileMeta {
  // the number of the max meta info
  public static final long IGINX_FILE_META_INDEX = 3L;
  public static final int MAGIC_NUMBER_INDEX = 1;
  // data type line number
  public static final int DATA_TYPE_INDEX = 2;
  // tagkv line number
  public static final int TAG_KV_INDEX = 3;
  public static final byte[] MAGIC_NUMBER = "IGINX".getBytes();
  public static final String MAGIC_NUMBER_NAME = "magic_number";
  public static final String DATA_TYPE_NAME = "data_type";
  public static final String TAG_KV_NAME = "tag_KV";
  private byte[] magicNumber;
  private DataType dataType;
  private Map<String, String> tags;

  public FileMeta() {
    this.dataType = null;
    this.tags = null;
  }

  public FileMeta(DataType dataType, Map<String, String> tag) {
    this.dataType = dataType;
    this.tags = tag;
  }

  public boolean ifTagEqual(Map<String, String> map) {
    if (map.size() != tags.size()) return false;

    for (String key : map.keySet()) {
      if (!tags.containsKey(key)) return false;
      String value1 = map.get(key);
      String value2 = tags.get(key);
      if (!value1.equals(value2)) return false;
    }

    return true;
  }

  public boolean ifContainTag(Map<String, String> map) {
    for (String key : map.keySet()) {
      if (this.tags == null || !this.tags.containsKey(key)) return false;
      String value = map.get(key);
      if (!this.tags.get(key).equals(value)) return false;
    }
    return true;
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
    if (tags.size() == 0) {
      this.tags = null;
    } else {
      this.tags = tags;
    }
  }

  public byte[] getMagicNumber() {
    return magicNumber;
  }

  public void setMagicNumber(byte[] magicNumber) {
    this.magicNumber = magicNumber;
  }
}
