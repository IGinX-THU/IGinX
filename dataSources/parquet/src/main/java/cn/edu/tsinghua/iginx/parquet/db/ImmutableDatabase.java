package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ImmutableDatabase<K extends Comparable<K>, F, T, V> extends AutoCloseable {
  Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException;

  Optional<Range<K>> range() throws StorageException;

  Map<F, T> schema() throws StorageException;
}
