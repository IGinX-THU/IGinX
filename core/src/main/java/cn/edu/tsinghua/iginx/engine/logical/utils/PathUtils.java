package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;

public class PathUtils {

  public static final String STAR = "*";

  public static final Character MIN_CHAR = '!';
  public static final Character MAX_CHAR = '~';

  public static ColumnsInterval trimColumnsInterval(ColumnsInterval columnsInterval) {
    String startColumn = columnsInterval.getStartColumn();
    if (startColumn.contains(STAR)) {
      if (startColumn.startsWith(STAR)) {
        startColumn = null;
      } else {
        startColumn = startColumn.substring(0, startColumn.indexOf(STAR)) + MIN_CHAR;
      }
    }

    String endColumn = columnsInterval.getEndColumn();
    if (endColumn.contains(STAR)) {
      if (endColumn.startsWith(STAR)) {
        endColumn = null;
      } else {
        endColumn = endColumn.substring(0, endColumn.indexOf(STAR)) + MAX_CHAR;
      }
    }

    if (startColumn != null && startColumn.equals(endColumn)) {
      startColumn += MIN_CHAR;
      endColumn += MAX_CHAR;
    }

    return new ColumnsInterval(startColumn, endColumn);
  }
}
