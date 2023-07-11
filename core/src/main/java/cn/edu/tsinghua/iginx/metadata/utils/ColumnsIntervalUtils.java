package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;

public class ColumnsIntervalUtils {

  public static ColumnsInterval fromString(String str) {
    String[] parts = str.split("-");
    assert parts.length == 2;
    return new ColumnsInterval(
        parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
  }
}
