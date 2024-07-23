package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Map;

public class Fields {

  private Fields() {
  }

  public static Field of(Column column) {
    String name = column.getPath();
    Map<String, String> tags = column.getTags();
    DataType dataType = column.getDataType();
    return new Field(name, dataType, tags);
  }
}
