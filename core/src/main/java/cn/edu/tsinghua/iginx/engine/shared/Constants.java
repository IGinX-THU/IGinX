package cn.edu.tsinghua.iginx.engine.shared;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {

  public static final String KEY = "key";

  public static final String ORDINAL = "ordinal";

  public static final String ALL_PATH = "*";
  public static final String ALL_PATH_SUFFIX = ".*";

  public static final String UDF_CLASS = "t";
  public static final String UDF_FUNC = "transform";

  public static final String WINDOW_START_COL = "window_start";
  public static final String WINDOW_END_COL = "window_end";

  // 保留列名，会在reorder时保留，并按原顺序出现在表的最前面
  public static final Set<String> RESERVED_COLS =
      new HashSet<>(Arrays.asList(WINDOW_START_COL, WINDOW_END_COL));
}
