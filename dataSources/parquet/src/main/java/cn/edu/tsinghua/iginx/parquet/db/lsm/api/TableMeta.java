package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;

import javax.annotation.Nullable;
import java.util.Map;

public interface TableMeta {
  Map<String, DataType> getSchema();

  Range<Long> getRange(String field);

  @Nullable
  Long getValueCount(String field);
}
