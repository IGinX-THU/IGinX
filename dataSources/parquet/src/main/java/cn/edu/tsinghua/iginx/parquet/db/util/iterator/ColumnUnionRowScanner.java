package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import javax.annotation.Nonnull;

public class ColumnUnionRowScanner<K extends Comparable<K>, F, V>
    implements Scanner<K, Scanner<F, V>> {

  private PriorityQueue<Map.Entry<F, Scanner<K, V>>> queue = null;

  private final Map<F, Scanner<K, V>> scanners;

  private K currentKey = null;

  private Scanner<F, V> currentScanner = null;

  public ColumnUnionRowScanner(Map<F, Scanner<K, V>> scanners) {
    this.scanners = scanners;
  }

  private void init() throws StorageException {
    this.queue = new PriorityQueue<>(Comparator.comparing(l -> l.getValue().key()));
    for (Map.Entry<F, Scanner<K, V>> entry : scanners.entrySet()) {
      if (entry.getValue().iterate()) {
        queue.add(entry);
      }
    }
  }

  @Nonnull
  @Override
  public K key() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    assert queue.peek() != null;
    return queue.peek().getValue().key();
  }

  @Nonnull
  @Override
  public Scanner<F, V> value() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    return currentScanner;
  }

  @Override
  public boolean iterate() throws StorageException {
    if (queue == null) {
      init();
    }

    if (currentScanner != null) {
      currentScanner.close();
      currentScanner = null;
    }
    if (queue.isEmpty()) {
      return false;
    }
    currentScanner = new QueueScanner();
    return true;
  }

  @Override
  public void close() throws StorageException {
    Map.Entry<F, Scanner<K, V>> entry;
    while ((entry = queue.poll()) != null) {
      entry.getValue().close();
    }
  }

  private class QueueScanner implements Scanner<F, V> {

    private K currentKey = null;

    @Nonnull
    @Override
    public F key() throws NoSuchElementException {
      if (queue.peek() == null) {
        throw new NoSuchElementException();
      }
      return queue.peek().getKey();
    }

    @Nonnull
    @Override
    public V value() throws NoSuchElementException {
      if (queue.peek() == null) {
        throw new NoSuchElementException();
      }
      return queue.peek().getValue().value();
    }

    @Override
    public boolean iterate() throws StorageException {
      if (queue.isEmpty()) {
        return false;
      }

      if (currentKey == null) {
        currentKey = queue.peek().getValue().key();
        return true;
      }

      if (currentKey.compareTo(queue.peek().getValue().key()) != 0) {
        return false;
      }

      Map.Entry<F, Scanner<K, V>> entry = queue.poll();
      assert entry != null;
      if (entry.getValue().iterate()) {
        queue.add(entry);
      }
      return !queue.isEmpty() && currentKey.compareTo(queue.peek().getValue().key()) == 0;
    }

    @Override
    public void close() throws StorageException {
      boolean hasNext;
      do {
        hasNext = iterate();
      } while (hasNext);
    }
  }
}
