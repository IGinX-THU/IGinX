package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

public class EmtpyHeadRowScanner<K extends Comparable<K>, F, V>
    implements Scanner<K, Scanner<F, V>> {

  private final K key;
  private boolean hasNext = true;

  public EmtpyHeadRowScanner(K key) {
    this.key = key;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public Scanner<F, V> value() {
    return EmptyScanner.getInstance();
  }

  @Override
  public boolean iterate() throws StorageException {
    boolean lastHasNext = hasNext;
    hasNext = false;
    return lastHasNext;
  }

  @Override
  public void close() throws StorageException {}
}
