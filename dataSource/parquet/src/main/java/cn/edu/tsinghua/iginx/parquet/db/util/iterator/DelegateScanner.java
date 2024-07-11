package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

public class DelegateScanner<K, V> implements Scanner<K, V> {
  private final Scanner<K, V> scanner;

  public DelegateScanner(Scanner<K, V> scanner) {
    this.scanner = scanner;
  }

  @Override
  public K key() {
    return scanner.key();
  }

  @Override
  public V value() {
    return scanner.value();
  }

  @Override
  public boolean iterate() throws StorageException {
    return scanner.iterate();
  }

  @Override
  public void close() throws StorageException {
    scanner.close();
  }
}
