package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class IteratorScanner<K, V> implements Scanner<K, V> {

  private Iterator<Map.Entry<K, V>> iterator;

  private K key;

  private V value;

  public IteratorScanner(Iterator<Map.Entry<K, V>> iterator) {
    this.iterator = iterator;
  }

  @Nonnull
  @Override
  public K key() throws NoSuchElementException {
    if (key == null) {
      throw new NoSuchElementException();
    }
    return key;
  }

  @Nonnull
  @Override
  public V value() throws NoSuchElementException {
    if (value == null) {
      throw new NoSuchElementException();
    }
    return value;
  }

  @Override
  public boolean iterate() {
    if (!iterator.hasNext()) {
      key = null;
      value = null;
      return false;
    }
    Map.Entry<K, V> entry = iterator.next();
    key = entry.getKey();
    value = entry.getValue();
    return true;
  }

  @Override
  public void close() {
    iterator = null;
  }
}
