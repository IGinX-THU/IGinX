package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Column {

  private String pathName;

  private final String physicalPath;

  private final DataType type;

  private final Map<Long, Object> data = new HashMap<>();

  public Column(String pathName, String physicalPath, DataType type) {
    this.pathName = pathName;
    this.physicalPath = physicalPath;
    this.type = type;
  }

  public void putData(long time, Object value) {
    data.put(time, value);
  }

  public void putBatchData(Map<Long, Object> batchData) {
    data.putAll(batchData);
  }

  public void removeData(long time) {
    data.remove(time);
  }

  public String getPathName() {
    return pathName;
  }

  public String getPhysicalPath() {
    return physicalPath;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }

  public void setPathName(String pathName) {
    this.pathName = pathName;
  }
}
