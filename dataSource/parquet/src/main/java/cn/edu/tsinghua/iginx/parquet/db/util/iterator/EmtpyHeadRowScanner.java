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

public class EmtpyHeadRowScanner<K extends Comparable<K>, F, V>
    implements Scanner<K, Scanner<F, V>> {

  private final K key;
  private boolean hasNext = true;

  public EmtpyHeadRowScanner(K key) {
    this.key = key;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public Scanner<F, V> value() {
    return EmptyScanner.getInstance();
  }

  @Override
  public boolean iterate() throws StorageException {
    boolean lastHasNext = hasNext;
    hasNext = false;
    return lastHasNext;
  }

  @Override
  public void close() throws StorageException {}
}
