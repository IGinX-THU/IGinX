package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import com.google.common.collect.Range;
import java.util.Map;

public interface TableMeta<K extends Comparable<K>, F, T, V> {
  Map<F, T> getSchema();

  /**
   * Get the range of the table. 若某个字段没有数据，则返回的range为null
   *
   * @return the range of the table, the key is the field name, the value is the range of the field
   */
  Map<F, Range<K>> getRanges();
}
