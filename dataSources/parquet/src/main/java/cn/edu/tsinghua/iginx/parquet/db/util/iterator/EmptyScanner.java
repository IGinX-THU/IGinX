package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class EmptyScanner<K, V> implements Scanner<K, V> {

  private static final Scanner<?, ?> EMPTY = new EmptyScanner<>();

  @SuppressWarnings("unchecked")
  public static <K, V> Scanner<K, V> getInstance() {
    return (Scanner<K, V>) EMPTY;
  }

  @Nonnull
  @Override
  public K key() {
    throw new NoSuchElementException();
  }

  @Nonnull
  @Override
  public V value() {
    throw new NoSuchElementException();
  }

  @Override
  public boolean iterate() throws StorageException {
    return false;
  }

  @Override
  public void close() throws StorageException {}
}
