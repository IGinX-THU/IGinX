package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import java.util.Map;
import javax.annotation.Nullable;

public interface TableMeta {
  Map<String, DataType> getSchema();

  Range<Long> getRange(String field);

  default Range<Long> getRange(Iterable<String> fields) {
    Range<Long> range = null;
    for (String field : fields) {
      Range<Long> fieldRange = getRange(field);
      if (range == null) {
        range = getRange(field);
      } else {
        range = range.span(fieldRange);
      }
    }
    return range;
  }

  @Nullable
  Long getValueCount(String field);
}
