package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

public interface RowScannerFactory<K extends Comparable<K>, F, V> {
  Scanner<K, Scanner<F, V>> create() throws StorageException;
}
