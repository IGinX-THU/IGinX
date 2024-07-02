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

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import com.google.common.collect.RangeSet;
import javax.annotation.Nonnull;

public class AreaFilterScanner<K extends Comparable<K>, F, V> implements Scanner<K, Scanner<F, V>> {

  private final Scanner<K, Scanner<F, V>> scanner;

  private final AreaSet<K, F> areas;

  public AreaFilterScanner(Scanner<K, Scanner<F, V>> scanner, AreaSet<K, F> exclusive) {
    this.scanner = scanner;
    this.areas = exclusive;
  }

  private K currentKey = null;
  private Scanner<F, V> currentValue = null;

  @Nonnull
  @Override
  public K key() {
    return currentKey;
  }

  @Nonnull
  @Override
  public Scanner<F, V> value() {
    return currentValue;
  }

  @Override
  public boolean iterate() throws StorageException {
    while (scanner.iterate()) {
      K key = scanner.key();
      if (!excludeKey(key)) {
        currentKey = key;
        currentValue = new RowFilterScanner(scanner.value(), areas);
        return true;
      }
    }
    currentKey = null;
    currentValue = null;
    return false;
  }

  @Override
  public void close() throws StorageException {
    scanner.close();
  }

  private boolean excludeKey(K key) {
    if (areas.getKeys().contains(key)) {
      return true;
    }
    return false;
  }

  private class RowFilterScanner implements Scanner<F, V> {

    private final Scanner<F, V> scanner;

    private final AreaSet<K, F> areas;

    private RowFilterScanner(Scanner<F, V> scanner, AreaSet<K, F> exclusive) {
      this.scanner = scanner;
      this.areas = exclusive;
    }

    private F currentField = null;
    private V currentValue = null;

    @Nonnull
    @Override
    public F key() {
      return currentField;
    }

    @Nonnull
    @Override
    public V value() {
      return currentValue;
    }

    @Override
    public boolean iterate() throws StorageException {
      while (scanner.iterate()) {
        F field = scanner.key();
        if (!excludeField(field)) {
          currentField = field;
          currentValue = scanner.value();
          return true;
        }
      }
      currentField = null;
      currentValue = null;
      return false;
    }

    @Override
    public void close() throws StorageException {}

    private boolean excludeField(F field) {
      if (areas.getFields().contains(field)) {
        return true;
      }
      RangeSet<K> rangeSet = areas.getSegments().get(field);
      if (rangeSet == null) {
        return false;
      }
      return rangeSet.contains(currentKey);
    }
  }
}
