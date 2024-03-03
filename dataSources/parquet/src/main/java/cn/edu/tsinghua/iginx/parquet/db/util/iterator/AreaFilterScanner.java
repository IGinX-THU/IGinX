/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
