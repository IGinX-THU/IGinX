package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import javax.annotation.Nullable;

public class DataUnits {

  private DataUnits() {}

  public static DataUnit of(boolean dummy, @Nullable String name) {
    DataUnit unit = new DataUnit(dummy);
    unit.setName(name);
    return unit;
  }
}
