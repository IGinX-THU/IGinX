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
import cn.edu.tsinghua.iginx.parquet.db.common.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import com.google.common.collect.Range;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable<K extends Comparable<K>, F, V, T> implements Table<K, F, V, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final DataBuffer<K, F, V> buffer;
  private final Map<F, T> schema;
  private final Map<String, String> extra;

  public MemoryTable(DataBuffer<K, F, V> buffer, Map<F, T> schema, Map<String, String> extra) {
    this.buffer = Objects.requireNonNull(buffer);
    this.schema = Objects.requireNonNull(schema);
    this.extra = Objects.requireNonNull(extra);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("schema=" + schema)
        .add("extra=" + extra)
        .add("buffer=" + buffer.range())
        .toString();
  }

  @Nonnull
  @Override
  public TableMeta<F, T> getMeta() {
    return new TableMeta<>(schema, extra);
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull Range<K> range, @Nullable Filter predicate)
      throws IOException {
    LOGGER.debug("read {} where {} from {},{}", fields, range, buffer.fields(), buffer.range());
    return buffer.scanRows(fields, range);
  }
}
