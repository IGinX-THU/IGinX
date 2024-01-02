package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import com.google.common.collect.RangeSet;
import java.util.Set;

public interface Database<K extends Comparable<K>, F, T, V> extends ImmutableDatabase<K, F, T, V> {

  void upsertRows(Scanner<K, Scanner<F, V>> scanner) throws NativeStorageException;
  // void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F,T> schema) throws
  // NativeStorageException;

  void upsertColumns(Scanner<F, Scanner<K, V>> scanner) throws NativeStorageException;
  // void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F,T> schema) throws
  // NativeStorageException;

  void delete(Set<F> fields, RangeSet<K> ranges) throws NativeStorageException;

  void deleteRows(Set<F> fields) throws NativeStorageException;

  void deleteColumns(RangeSet<K> ranges) throws NativeStorageException;

  void clear() throws NativeStorageException;
}
