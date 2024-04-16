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
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.MemColumn;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.*;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.parquet.util.SingleCache;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable<K extends Comparable<K>, F, T, V>
    implements Table<K, F, T, V>, NoexceptAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final LinkedHashMap<Field, MemColumn.Snapshot> columns;
  private final Map<F, Field> fieldMap = new HashMap<>();
  private final SingleCache<TableMeta<K, F, T, V>> meta =
      new SingleCache<>(() -> new MemoryTableMeta<>(getSchema(), getRanges()));

  public MemoryTable(@WillCloseWhenClosed LinkedHashMap<Field, MemColumn.Snapshot> columns) {
    this.columns = new LinkedHashMap<>(columns);
    for (Field field : columns.keySet()) {
      fieldMap.put(getFieldString(field), field);
    }
  }

  private Map<F, T> getSchema() {
    return (Map) ArrowFields.toIginxSchema(columns.keySet());
  }

  private Map<F, Range<K>> getRanges() {
    return columns.keySet().stream()
        .collect(Collectors.toMap(this::getFieldString, this::getRange));
  }

  private F getFieldString(Field field) {
    return (F) TagKVUtils.toFullName(ArrowFields.toColumnKey(field));
  }

  private Range<K> getRange(Field field) {
    MemColumn.Snapshot snapshot = columns.get(field);
    RangeSet<Long> ranges = snapshot.getRanges();
    if (ranges.isEmpty()) {
      return (Range<K>) Range.closed(0L, 0L);
    }
    return (Range<K>) ranges.span();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("meta=" + meta)
        .toString();
  }

  @Override
  public TableMeta<K, F, T, V> getMeta() {
    return meta.get();
  }

  @Override
  public Scanner<K, Scanner<F, V>> scan(
      Set<F> fields, RangeSet<K> ranges, @Nullable Filter superSetPredicate) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("read {} where {} from {}", fields, ranges, meta);
    }
    Map<F, Scanner<K, V>> columns = new HashMap<>();
    for (F field : fields) {
      if (!fieldMap.containsKey(field)) {
        continue;
      }
      Field arrowField = fieldMap.get(field);
      MemColumn.Snapshot snapshot = this.columns.get(arrowField);
      columns.put(field, scan(snapshot, ranges));
    }
    return new ColumnUnionRowScanner<>(columns);
  }

  private Scanner<K, V> scan(MemColumn.Snapshot snapshot, RangeSet<K> ranges) {
    if (ranges.isEmpty()) {
      return new EmptyScanner<>();
    }
    MemColumn.Snapshot sliced = snapshot.slice((RangeSet<Long>) ranges);
    return (Scanner<K, V>)
        new ListenCloseScanner<>(new IteratorScanner<>(sliced.iterator()), sliced::close);
  }

  @Override
  public void close() {
    columns.values().forEach(MemColumn.Snapshot::close);
  }

  public static class MemoryTableMeta<K extends Comparable<K>, F, T, V>
      implements TableMeta<K, F, T, V> {

    private final Map<F, T> schema;
    private final Map<F, Range<K>> ranges;

    MemoryTableMeta(Map<F, T> schema, Map<F, Range<K>> ranges) {
      this.schema = Collections.unmodifiableMap(schema);
      this.ranges = Collections.unmodifiableMap(ranges);
    }

    public Map<F, T> getSchema() {
      return schema;
    }

    public Map<F, Range<K>> getRanges() {
      return ranges;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", MemoryTableMeta.class.getSimpleName() + "[", "]")
          .add("schema=" + schema)
          .add("ranges=" + ranges)
          .toString();
    }
  }
}
