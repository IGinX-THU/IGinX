package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Date;
import java.util.Map;

public class FileMeta {
  // the number of the max meta info
  public static final long IGINX_FILE_META_INDEX = 10L;
  // data type line number
  public static final int DATA_TYPE_INDEX = 1;
  // tagkv line number
  public static final int TAG_KV_INDEX = 2;
  public static final String DATA_TYPE_NAME = "data_type";
  public static final String TAG_KV_NAME = "tag_KV";
  private DataType dataType;
  private Map<String, String> tag;
  private Date createTime;

  public FileMeta() {
    this.dataType = null;
    this.tag = null;
  }

  public FileMeta(DataType dataType, Map<String, String> tag) {
    this.dataType = dataType;
    this.tag = tag;
  }

  public boolean ifTagEqual(Map<String, String> map) {
    if (map.size() != tag.size()) return false;

    for (String key : map.keySet()) {
      if (!tag.containsKey(key)) return false;
      String value1 = map.get(key);
      String value2 = tag.get(key);
      if (!value1.equals(value2)) return false;
    }

    return true;
  }

  public boolean ifContainTag(Map<String, String> map) {
    for (String key : map.keySet()) {
      if (this.tag == null || !this.tag.containsKey(key)) return false;
      String value = map.get(key);
      if (!this.tag.get(key).equals(value)) return false;
    }
    return true;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public Map<String, String> getTag() {
    return tag;
  }

  public void setTag(Map<String, String> tag) {
    this.tag = tag;
  }
}
