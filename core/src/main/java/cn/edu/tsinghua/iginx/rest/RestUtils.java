package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class RestUtils {

  public static final String CATEGORY = "category"; // "category";
//  public static final String CATEGORY = "" + '\u2E84'; // "category";
  public static final long TOP_KEY = 9223372036854775804L;
  public static final long DESCRIPTION_KEY = 9223372036854775805L;
  public static final long TITLE_KEY = 9223372036854775806L;
  public static final long MAX_KEY = 9223372036854775807L;
  public static final long ANNOTATION_START_KEY = 10L;
  public static final String ANNOTATION_SEQUENCE = "TITLE.DESCRIPTION";

  public static DataType checkType(SessionQueryDataSet sessionQueryDataSet) {
    int n = sessionQueryDataSet.getKeys().length;
    int m = sessionQueryDataSet.getPaths().size();
    int ret = 0;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
          if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Integer
              || sessionQueryDataSet.getValues().get(i).get(j) instanceof Long) {
            ret = Math.max(ret, 1);
          } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Float
              || sessionQueryDataSet.getValues().get(i).get(j) instanceof Double) {
            ret = Math.max(ret, 2);
          } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof byte[]) {
            ret = 3;
          }
        }
      }
    }
    switch (ret) {
      case 0:
        return DataType.BOOLEAN;
      case 1:
        return DataType.LONG;
      case 2:
        return DataType.DOUBLE;
      case 3:
      default:
        return DataType.BINARY;
    }
  }

  public static long getInterval(long key, long startKey, long duration) {
    return (key - startKey) / duration;
  }

  public static long getIntervalStart(long key, long startKey, long duration) {
    return (key - startKey) / duration * duration + startKey;
  }
}
