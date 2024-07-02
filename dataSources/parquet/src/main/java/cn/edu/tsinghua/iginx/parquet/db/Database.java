package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.Map;

public interface Database<K extends Comparable<K>, F, T, V> extends ImmutableDatabase<K, F, T, V> {

  void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema) throws StorageException;

  void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema) throws StorageException;

  void delete(AreaSet<K, F> areas) throws StorageException;

  void clear() throws StorageException;
}
