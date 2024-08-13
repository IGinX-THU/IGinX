package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;

import java.util.List;

public class DataTargets {

  public static DataTarget subTarget(DataTarget target, String prefix) {
    List<String> patterns = Patterns.filterByPrefix(target.getPatterns(), prefix);
    Filter filter = Filters.startWith(target.getFilter(), prefix);
    return new DataTarget(filter, patterns, target.getTagFilter());
  }
}
