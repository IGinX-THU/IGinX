package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class Scanners {

  private static class EmptyScanner<K, V> implements Scanner<K, V> {
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
    public boolean iterate() throws NativeStorageException {
      return false;
    }

    @Override
    public void close() throws NativeStorageException {}
  }

  private static final Scanner<?, ?> EMPTY = new EmptyScanner<>();

  @SuppressWarnings("unchecked")
  public static <K, V> Scanner<K, V> empty() {
    return (Scanner<K, V>) EMPTY;
  }
}
