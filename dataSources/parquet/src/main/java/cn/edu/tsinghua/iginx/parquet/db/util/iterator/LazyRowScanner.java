package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

import java.util.Objects;

public class LazyRowScanner<K extends Comparable<K>, F, V> implements Scanner<K, Scanner<F, V>> {

  private final RowScannerFactory<K, F, V> factory;

  public LazyRowScanner(RowScannerFactory<K, F, V> factory) {
    this.factory = factory;
  }

  private Scanner<K, Scanner<F, V>> scanner;

  @Override
  public K key() {
    return scanner.key();
  }

  @Override
  public Scanner<F, V> value() {
    return scanner.value();
  }

  @Override
  public boolean iterate() throws StorageException {
    if (scanner == null) {
      scanner = factory.create();
    }
    return scanner.iterate();
  }

  @Override
  public void close() throws StorageException {
    if (scanner != null) {
      scanner.close();
    }
  }
}
