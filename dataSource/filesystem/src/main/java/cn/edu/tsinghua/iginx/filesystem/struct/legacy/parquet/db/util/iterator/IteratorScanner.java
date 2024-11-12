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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class IteratorScanner<K, V> implements Scanner<K, V> {

  private Iterator<Map.Entry<K, V>> iterator;

  private K key;

  private V value;

  public IteratorScanner(Iterator<Map.Entry<K, V>> iterator) {
    this.iterator = iterator;
  }

  @Override
  public K key() throws NoSuchElementException {
    if (key == null) {
      throw new NoSuchElementException();
    }
    return key;
  }

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
