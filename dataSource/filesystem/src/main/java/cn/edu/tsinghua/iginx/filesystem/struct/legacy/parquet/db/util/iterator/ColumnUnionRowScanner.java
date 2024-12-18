/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception.StorageException;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import org.apache.arrow.util.AutoCloseables;

public class ColumnUnionRowScanner<K extends Comparable<K>, F, V>
    implements Scanner<K, Scanner<F, V>> {

  private PriorityQueue<Map.Entry<F, Scanner<K, V>>> queue = null;

  private final Map<F, Scanner<K, V>> scanners;

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

  @Override
  public K key() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    assert queue.peek() != null;
    return queue.peek().getValue().key();
  }

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
    try {
      AutoCloseables.close(scanners.values());
    } catch (RuntimeException | StorageException e) {
      throw e;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private class QueueScanner implements Scanner<F, V> {

    private K currentKey = null;

    @Override
    public F key() throws NoSuchElementException {
      if (queue.peek() == null) {
        throw new NoSuchElementException();
      }
      return queue.peek().getKey();
    }

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
