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

public interface Scanner<K, V> extends AutoCloseable {
  @Nonnull
  K key() throws NoSuchElementException;

  @Nonnull
  V value() throws NoSuchElementException;

  boolean iterate() throws NativeStorageException;

  @Override
  void close() throws NativeStorageException;
}
