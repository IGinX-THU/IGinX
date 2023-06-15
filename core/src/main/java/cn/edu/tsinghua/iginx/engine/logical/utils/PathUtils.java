package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;

public class PathUtils {

  public static final String STAR = "*";

  public static final Character MIN_CHAR = '!';
  public static final Character MAX_CHAR = '~';

  public static ColumnsRange trimTimeSeriesInterval(ColumnsRange tsInterval) {
    String startPath = tsInterval.getStartColumn();
    if (startPath.contains(STAR)) {
      if (startPath.startsWith(STAR)) {
        startPath = null;
      } else {
        startPath = startPath.substring(0, startPath.indexOf(STAR)) + MIN_CHAR;
      }
    }

    String endPath = tsInterval.getEndColumn();
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
