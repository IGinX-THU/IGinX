package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class ConcatScanner<K extends Comparable<K>, V> implements Scanner<K, V> {

  private final Iterator<Scanner<K, V>> scannerIterator;

  private Scanner<K, V> currentScanner;

  public ConcatScanner(Iterator<Scanner<K, V>> iterator) {
    this.scannerIterator = iterator;
  }

  @Nonnull
  @Override
  public K key() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    return currentScanner.key();
  }

  @Nonnull
  @Override
  public V value() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    return currentScanner.value();
  }

  @Override
  public boolean iterate() throws StorageException {
    if (currentScanner != null && currentScanner.iterate()) {
      return true;
    }
    while (scannerIterator.hasNext()) {
      currentScanner = scannerIterator.next();
      if (currentScanner.iterate()) {
        return true;
      }
    }
    currentScanner = null;
    return false;
  }

  @Override
  public void close() throws StorageException {
    while (scannerIterator.hasNext()) {
      scannerIterator.next().close();
    }
  }
}
