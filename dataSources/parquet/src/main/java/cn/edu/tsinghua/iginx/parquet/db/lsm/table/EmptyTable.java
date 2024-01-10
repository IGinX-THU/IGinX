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

package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.common.scanner.EmptyScanner;
import cn.edu.tsinghua.iginx.parquet.common.scanner.Scanner;
import com.google.common.collect.Range;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EmptyTable<K extends Comparable<K>, F, V, T> implements Table<K, F, V, T> {
  private static final EmptyTable<?, ?, ?, ?> EMPTY = new EmptyTable<>();

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, F, T, V> EmptyTable<K, F, T, V> getInstance() {
    return (EmptyTable<K, F, T, V>) EMPTY;
  }

  @Nonnull
  @Override
  public TableMeta<F, T> getMeta() {
    return new TableMeta<>(new HashMap<>(), new HashMap<>());
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull Range<K> range, @Nullable Filter predicate)
      throws IOException {
    return EmptyScanner.getInstance();
  }
}
