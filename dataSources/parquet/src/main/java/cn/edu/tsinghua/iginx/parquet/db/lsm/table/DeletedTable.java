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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DeletedTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {

  private final Table<K, F, T, V> table;

  private final AreaSet<K, F> deletedAreaSet;

  private final TableMeta<K, F, T, V> meta;

  public DeletedTable(Table<K, F, T, V> table, AreaSet<K, F> deleted) throws IOException {
    this.table = table;
    this.deletedAreaSet = deleted;
    this.meta = new DeletedTableMeta<>(table.getMeta(), deleted);
  }

  @Nonnull
  @Override
  public TableMeta<K, F, T, V> getMeta() throws IOException {
    return meta;
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> range, @Nullable Filter predicate)
      throws IOException {
    return new AreaFilterScanner<>(table.scan(fields, range, predicate), deletedAreaSet);
  }
}
