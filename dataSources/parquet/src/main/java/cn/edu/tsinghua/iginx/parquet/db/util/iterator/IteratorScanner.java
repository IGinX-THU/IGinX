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
