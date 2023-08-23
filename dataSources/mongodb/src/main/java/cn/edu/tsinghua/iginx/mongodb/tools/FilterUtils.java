package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.List;

public class FilterUtils {
  public static boolean isEmpty(List<KeyRange> range) {
    return range == null || range.isEmpty();
  }

  public static boolean isEmpty(TagFilter tagFilter) {
    return tagFilter == null;
  }
}
