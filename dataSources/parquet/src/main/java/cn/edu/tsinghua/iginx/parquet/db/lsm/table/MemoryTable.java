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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.ConcatScanner;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final DataBuffer<K, F, V> buffer;
  private final Map<F, T> schema;

  public MemoryTable(DataBuffer<K, F, V> buffer, Map<F, T> types) {
    this.buffer = Objects.requireNonNull(buffer);
    this.schema = Objects.requireNonNull(types);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("schema=" + schema)
        .add("buffer=" + buffer.ranges())
        .toString();
  }

  @Nonnull
  @Override
  public MemoryTableMeta<K, F, T, V> getMeta() {
    return new MemoryTableMeta<>(schema, buffer.ranges());
  }

  @Nonnull
  @Override
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges, @Nullable Filter predicate)
      throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("read {} where {} from {},{}", fields, ranges, buffer.fields(), buffer.ranges());
    }
    List<Scanner<K, Scanner<F, V>>> scanners = new ArrayList<>();
    for (Range<K> range : ranges.asRanges()) {
      scanners.add(buffer.scanRows(fields, range));
    }
    return new ConcatScanner<>(scanners.iterator());
  }
}
