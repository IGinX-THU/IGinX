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

package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class Scanners {

  private static class EmptyScanner<K, V> implements Scanner<K, V> {
    @Nonnull
    @Override
    public K key() {
      throw new NoSuchElementException();
    }

    @Nonnull
    @Override
    public V value() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean iterate() throws NativeStorageException {
      return false;
    }

    @Override
    public void close() throws NativeStorageException {}
  }

  private static final Scanner<?, ?> EMPTY = new EmptyScanner<>();

  @SuppressWarnings("unchecked")
  public static <K, V> Scanner<K, V> empty() {
    return (Scanner<K, V>) EMPTY;
  }
}
