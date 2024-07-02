package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.*;
import javax.annotation.Nonnull;

public class RowUnionScanner<K extends Comparable<K>, F, V> implements Scanner<K, Scanner<F, V>> {

  private final PriorityQueue<Map.Entry<Scanner<K, Scanner<F, V>>, Long>> queue;

  public RowUnionScanner(Iterable<Scanner<K, Scanner<F, V>>> scanners) throws StorageException {
    Comparator<Map.Entry<Scanner<K, Scanner<F, V>>, Long>> comparing =
        Comparator.comparing(e -> e.getKey().key());
    this.queue = new PriorityQueue<>(comparing.thenComparing(Map.Entry::getValue));

    long i = 0;
    for (Scanner<K, Scanner<F, V>> scanner : scanners) {
      if (scanner.iterate()) {
        queue.add(new AbstractMap.SimpleImmutableEntry<>(scanner, i));
        i++;
      }
    }
  }

  private Scanner<F, V> currentRow = null;

  private K currentKey = null;

  @Nonnull
  @Override
  public K key() throws NoSuchElementException {
    if (currentKey == null) {
      throw new NoSuchElementException();
    }
    return currentKey;
  }

  @Nonnull
  @Override
  public Scanner<F, V> value() throws NoSuchElementException {
    if (currentRow == null) {
      throw new NoSuchElementException();
    }

    return currentRow;
  }

  @Override
  public boolean iterate() throws StorageException {
    Map<F, V> row = new HashMap<>();
    if (queue.isEmpty()) {
      currentRow = null;
      currentKey = null;
      return false;
    }
    currentKey = queue.peek().getKey().key();
    while (!queue.isEmpty() && currentKey.compareTo(queue.peek().getKey().key()) == 0) {
      Map.Entry<Scanner<K, Scanner<F, V>>, Long> entry = queue.poll();
      assert entry != null;
      Scanner<F, V> scanner = entry.getKey().value();
      while (scanner.iterate()) {
        row.putIfAbsent(scanner.key(), scanner.value());
      }
      if (entry.getKey().iterate()) {
        queue.add(entry);
      }
    }
    currentRow =
        new Scanner<F, V>() {
          private final Iterator<Map.Entry<F, V>> iterator = row.entrySet().iterator();

          private F key;

          private V value;

          @Nonnull
          @Override
          public F key() throws NoSuchElementException {
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
          public boolean iterate() throws StorageException {
            if (!iterator.hasNext()) {
              key = null;
              value = null;
              return false;
            }
            Map.Entry<F, V> entry = iterator.next();
            key = entry.getKey();
            value = entry.getValue();
            return true;
          }

          @Override
          public void close() throws StorageException {}
        };
    return true;
  }

  @Override
  public void close() throws StorageException {
    for (Map.Entry<Scanner<K, Scanner<F, V>>, Long> entry : queue) {
      entry.getKey().close();
    }
  }
}
