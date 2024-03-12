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

package cn.edu.tsinghua.iginx.parquet.db.lsm;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Prefetch;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.util.StorageShared;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

class QueriedTableSequence<K extends Comparable<K>, F, T, V> implements Prefetch.Prefetchable {

  private final StorageShared shared;
  private final List<String> tableNames;
  private final TableStorage<K, F, T, V> storage;
  private final Executor executor = Executors.newCachedThreadPool();

  QueriedTableSequence(
      StorageShared shared, List<String> tableNames, TableStorage<K, F, T, V> storage) {
    this.shared = shared;
    this.tableNames = tableNames;
    this.storage = storage;
  }

  @Override
  public long count() {
    return tableNames.size();
  }

  @Override
  public long estimateCost(long id) {
    return shared.getStorageProperties().getWriteBufferSize();
  }

  @Override
  public Object fetch(long id) throws IOException {
    String tableName = tableNames.get((int) id);
    Table<K, F, T, V> table = storage.get(tableName);
    return new Entry(table);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void execute(Runnable command) {
    executor.execute(command);
  }

  class Entry {
    private final Table<K, F, T, V> table;

    Entry(Table<K, F, T, V> table) {
      this.table = table;
    }

    Table<K, F, T, V> getTable() {
      return table;
    }
  }
}
