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

package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import com.google.common.collect.RangeSet;
import java.util.Map;
import java.util.Set;

public interface Database<K extends Comparable<K>, F, T, V> extends ImmutableDatabase<K, F, T, V> {

  void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema) throws StorageException;

  void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema) throws StorageException;

  void delete(Set<F> fields, RangeSet<K> ranges) throws StorageException;

  void delete(Set<F> fields) throws StorageException;

  void delete(RangeSet<K> ranges) throws StorageException;

  void clear() throws StorageException;
}
