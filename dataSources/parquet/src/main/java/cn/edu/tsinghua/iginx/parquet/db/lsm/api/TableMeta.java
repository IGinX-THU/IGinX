package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import com.google.common.collect.Range;
import java.util.Map;

public interface TableMeta<K extends Comparable<K>, F, T, V> {
  Map<F, T> getSchema();

  Map<F, Range<K>> getRanges();
}
