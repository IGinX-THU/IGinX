/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
