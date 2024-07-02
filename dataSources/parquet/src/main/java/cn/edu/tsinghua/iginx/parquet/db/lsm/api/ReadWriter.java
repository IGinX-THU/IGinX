package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, T, V> {

  void flush(String name, TableMeta<K, F, T, V> meta, Scanner<K, Scanner<F, V>> scanner)
      throws IOException;

  TableMeta<K, F, T, V> readMeta(String name) throws IOException;

  Scanner<K, Scanner<F, V>> scanData(
      String name, Set<F> fields, RangeSet<K> ranges, Filter predicate) throws IOException;

  void delete(String name, AreaSet<K, F> areas) throws IOException;

  void delete(String name);

  Iterable<String> tableNames() throws IOException;

  void clear() throws IOException;
}
