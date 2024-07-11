/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable implements Table, NoexceptAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final LinkedHashMap<Field, MemColumn.Snapshot> columns;
  private final Map<String, Field> fieldMap = new HashMap<>();
  private final SingleCache<TableMeta> meta =
      new SingleCache<>(() -> new MemoryTableMeta(getSchema(), getRanges(), getCounts()));

  public MemoryTable(@WillCloseWhenClosed LinkedHashMap<Field, MemColumn.Snapshot> columns) {
    this.columns = new LinkedHashMap<>(columns);
    for (Field field : columns.keySet()) {
      fieldMap.put(getFieldString(field), field);
    }
  }

  public static MemoryTable empty() {
    return new MemoryTable(new LinkedHashMap<>());
  }

  public Set<Field> getFields() {
    return columns.keySet();
  }

  public boolean isEmpty() {
    return columns.isEmpty();
  }

  private Map<String, DataType> getSchema() {
    return (Map) ArrowFields.toIginxSchema(columns.keySet());
  }

  private Map<String, Range<Long>> getRanges() {
    return columns.keySet().stream()
        .collect(Collectors.toMap(this::getFieldString, this::getRange));
  }

  private Map<String, Long> getCounts() {
    // TODO: give statistics
    return Collections.emptyMap();
  }

  private String getFieldString(Field field) {
    return TagKVUtils.toFullName(ArrowFields.toColumnKey(field));
  }

  private Range<Long> getRange(Field field) {
    MemColumn.Snapshot snapshot = columns.get(field);
    RangeSet<Long> ranges = snapshot.getRanges();
    if (ranges.isEmpty()) {
      return Range.closed(0L, 0L);
    }
    return ranges.span();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("meta=" + meta)
        .toString();
  }

  @Override
  public TableMeta getMeta() {
    return meta.get();
  }

  @Override
  public Scanner<Long, Scanner<String, Object>> scan(
      Set<String> fields, RangeSet<Long> ranges, @Nullable Filter superSetPredicate) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("read {} where {} from {}", fields, ranges, meta);
    }
    Map<String, Scanner<Long, Object>> columns = new HashMap<>();
    for (String field : fields) {
      if (!fieldMap.containsKey(field)) {
        continue;
      }
      Field arrowField = fieldMap.get(field);
      MemColumn.Snapshot snapshot = this.columns.get(arrowField);
      columns.put(field, scan(snapshot, ranges));
    }
    return new ColumnUnionRowScanner<>(columns);
  }

  private Scanner<Long, Object> scan(MemColumn.Snapshot snapshot, RangeSet<Long> ranges) {
    if (ranges.isEmpty()) {
      return new EmptyScanner<>();
    }
    MemColumn.Snapshot sliced = snapshot.slice(ranges);
    return new ListenCloseScanner<>(new IteratorScanner<>(sliced.iterator()), sliced::close);
  }

  @Override
  public void close() {
    columns.values().forEach(MemColumn.Snapshot::close);
    columns.clear();
  }

  public MemoryTable subTable(Field field) {
    LinkedHashMap<Field, MemColumn.Snapshot> subColumns = new LinkedHashMap<>();
    subColumns.put(field, columns.get(field));
    return new MemoryTable(subColumns) {
      @Override
      public void close() {
        // do nothing
      }
    };
  }

  public static class MemoryTableMeta implements TableMeta {

    private final Map<String, DataType> schema;
    private final Map<String, Range<Long>> ranges;
    private final Map<String, Long> counts;

    MemoryTableMeta(
        Map<String, DataType> schema, Map<String, Range<Long>> ranges, Map<String, Long> counts) {
      this.schema = Collections.unmodifiableMap(schema);
      this.ranges = Collections.unmodifiableMap(ranges);
      this.counts = Collections.unmodifiableMap(counts);
    }

    public Map<String, DataType> getSchema() {
      return schema;
    }

    @Override
    public Range<Long> getRange(String field) {
      if (!schema.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return Objects.requireNonNull(ranges.get(field));
    }

    @Override
    public Long getValueCount(String field) {
      if (!schema.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return counts.get(field);
    }

    public Map<String, Range<Long>> getRanges() {
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
