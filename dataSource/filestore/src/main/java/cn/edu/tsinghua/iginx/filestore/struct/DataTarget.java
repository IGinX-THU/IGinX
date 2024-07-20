package cn.edu.tsinghua.iginx.filestore.struct;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.List;
import lombok.Value;

@Value
public class DataTarget {
  /** filter rows, null only if return all rows */
  Filter filter;
  /** filter columns, null only if return all columns, empty list only if not filtered */
  List<String> patterns;
  /** filter tags, null only if return all tags */
  TagFilter tagFilter;
}
