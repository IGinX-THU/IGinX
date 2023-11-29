package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;

import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import javax.annotation.Nonnull;

public class ColumnUnionRowScanner<K extends Comparable<K>, F, V>
    implements Scanner<K, Scanner<F, V>> {

  private final PriorityQueue<Map.Entry<F, Scanner<K, V>>> queue;

  private K currentKey = null;

  private Scanner<F, V> currentScanner = null;

  public ColumnUnionRowScanner(Map<F, Scanner<K, V>> scanners) throws NativeStorageException {
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
  public boolean iterate() throws NativeStorageException {
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
  public void close() throws NativeStorageException {
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
    public boolean iterate() throws NativeStorageException {
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
    public void close() throws NativeStorageException {
      boolean hasNext;
      do {
        hasNext = iterate();
      } while (hasNext);
    }
  }
}
