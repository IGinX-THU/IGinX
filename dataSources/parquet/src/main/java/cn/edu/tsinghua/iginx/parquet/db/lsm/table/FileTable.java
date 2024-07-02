package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileTable.class);
  private final String tableName;

  private final ReadWriter<K, F, T, V> readWriter;

  public FileTable(String tableName, ReadWriter<K, F, T, V> readWriter) {
    this.tableName = tableName;
    this.readWriter = readWriter;
  }

  @Override
  @Nonnull
  public TableMeta<K, F, T, V> getMeta() throws IOException {
    return readWriter.readMeta(tableName);
  }

  @Override
  @Nonnull
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges, @Nullable Filter predicate)
      throws IOException {
    LOGGER.debug("read {} where {} & {} from {}", fields, ranges, predicate, tableName);
    return readWriter.scanData(tableName, fields, ranges, predicate);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FileTable.class.getSimpleName() + "[", "]")
        .add("tableName='" + tableName + "'")
        .add("readWriter=" + readWriter)
        .toString();
  }
}
