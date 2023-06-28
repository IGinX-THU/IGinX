package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;

public class PathUtils {

  public static final String STAR = "*";

  public static final Character MIN_CHAR = '!';
  public static final Character MAX_CHAR = '~';

  public static ColumnsInterval trimColumnsInterval(ColumnsInterval columnsInterval) {
    String startPath = columnsInterval.getStartColumn();
    if (startPath.contains(STAR)) {
      if (startPath.startsWith(STAR)) {
        startPath = null;
      } else {
        startPath = startPath.substring(0, startPath.indexOf(STAR)) + MIN_CHAR;
      }
    }

    String endPath = columnsInterval.getEndColumn();
    if (endPath.contains(STAR)) {
      if (endPath.startsWith(STAR)) {
        endPath = null;
      } else {
        endPath = endPath.substring(0, endPath.indexOf(STAR)) + MAX_CHAR;
      }
    }

    return new ColumnsInterval(startPath, endPath);
  }
}
