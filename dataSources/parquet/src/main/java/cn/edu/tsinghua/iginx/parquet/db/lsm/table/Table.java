package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Table<K extends Comparable<K>, F, T, V> {
  @Nonnull
  TableMeta<K, F, T, V> getMeta() throws IOException;

  @Nonnull
  Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> range, @Nullable Filter predicate)
      throws IOException;

  @Nonnull
  default Scanner<K, Scanner<F, V>> scan(@Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges)
      throws IOException {
    return scan(fields, ranges, null);
  }
}
