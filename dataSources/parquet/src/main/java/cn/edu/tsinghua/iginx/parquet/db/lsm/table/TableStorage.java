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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class TableStorage implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);

  private final TableIndex tableIndex;
  private final ReadWriter readWriter;
  private long sqnBase;

  public TableStorage(Shared shared, ReadWriter readWriter) throws IOException {
    this.readWriter = readWriter;

    Iterable<String> tableNames = readWriter.tableNames();
    String last =
        StreamSupport.stream(tableNames.spliterator(), false)
            .max(Comparator.naturalOrder())
            .orElse("0-0");
    this.sqnBase = getSeq(last) + 1;
    this.tableIndex = new TableIndex(this);
  }

  static long getSeq(String tableName) {
    Pattern pattern = Pattern.compile("^(\\d+)-.*$");
    Matcher matcher = pattern.matcher(tableName);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(1));
    } else {
      throw new IllegalArgumentException("invalid table name: " + tableName);
    }
  }

  public String getTableName(long sqn, String suffix) {
    return String.format("%019d-%s", sqnBase + sqn, suffix);
  }

  public List<String> flush(long sqn, String suffix, MemoryTable table)
      throws InterruptedException {
    if (table.isEmpty()) {
      return Collections.emptyList();
    }
    String name = getTableName(sqn, suffix);
    TableMeta meta = table.getMeta();
    try (Scanner<Long, Scanner<String, Object>> scanner =
             table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
      readWriter.flush(name, meta, scanner);
    } catch (IOException | StorageException e) {
      LOGGER.error("flush table {} failed", name, e);
    }
    return Collections.singletonList(name);
  }

  public void commit(String table, AreaSet<Long, Field> tombstone) {
    AreaSet<Long, String> innerTombstone = ArrowFields.toInnerAreas(tombstone);
    try {
      if (innerTombstone.isAll()) {
        readWriter.delete(table);
        return;
      }
      if (!innerTombstone.isEmpty()) {
        readWriter.delete(table, innerTombstone);
      }
      TableMeta meta = readWriter.readMeta(table);
      tableIndex.addTable(table, meta);
    } catch (IOException e) {
      LOGGER.error("commit table {} failed", table, e);
    }
  }

  public void clear() {
    sqnBase = 0;
    tableIndex.clear();
    try {
      readWriter.clear();
    } catch (IOException e) {
      LOGGER.error("clear failed", e);
    }
  }

  public TableMeta getMeta(String tableName) throws IOException {
    return new FileTable(tableName, readWriter).getMeta();
  }

  public void delete(AreaSet<Long, String> areas) throws IOException {
    Set<String> tables = tableIndex.find(areas);
    tableIndex.delete(areas);
    for (String tableName : tables) {
      readWriter.delete(tableName, areas);
    }
  }

  public Iterable<String> tableNames() throws IOException {
    return readWriter.tableNames();
  }

  @Override
  public void close() {
  }

  public Map<String, DataType> schema() {
    return tableIndex.getType();
  }

  public void declareFields(Map<String, DataType> schema) throws TypeConflictedException {
    tableIndex.declareFields(schema);
  }

  public DataBuffer<Long, String, Object> query(
      Set<String> fields, RangeSet<Long> ranges, Filter filter)
      throws StorageException, IOException {

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();

    Set<String> tables = tableIndex.find(areas);
    List<String> sortedTableNames = new ArrayList<>(tables);
    sortedTableNames.sort(Comparator.naturalOrder());

    for (String tableName : sortedTableNames) {
      try (Scanner<Long, Scanner<String, Object>> scanner = scan(tableName, fields, ranges)) {
        buffer.putRows(scanner);
      }
    }

    return buffer;
  }

  private Scanner<Long, Scanner<String, Object>> scan(
      String tableName, Set<String> fields, RangeSet<Long> ranges) throws IOException {
    return new FileTable(tableName, readWriter).scan(fields, ranges);
  }

  public Map<String, Long> count(Set<String> innerFields) throws StorageException, IOException {
    Map<String, Long> counts = new HashMap<>();

    for (String field : innerFields) {
      long count = count(field);
      counts.put(field, count);
    }

    return counts;
  }

  public long count(String field) throws StorageException, IOException {
    Set<String> fields = Collections.singleton(field);
    RangeSet<Long> ranges = ImmutableRangeSet.of(Range.all());

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    Set<String> tables = tableIndex.find(areas);

    List<String> sortedTableNames = new ArrayList<>(tables);
    sortedTableNames.sort(Comparator.naturalOrder());

    RangeSet<Long> rangeSet = TreeRangeSet.create();
    long totalCount = 0;
    for (String tableName : sortedTableNames) {
      TableMeta meta = readWriter.readMeta(tableName);
      Range<Long> range = meta.getRange(field);
      Long count = meta.getValueCount(field);
      if (count == null || rangeSet.intersects(range)) {
        LOGGER.debug("count {} by scan", field);
        return getOverlapCount(field, sortedTableNames, fields, ranges);
      } else {
        rangeSet.add(range);
        totalCount += count;
      }
    }
    LOGGER.debug("count {} by meta", field);
    return totalCount;
  }

  private long getOverlapCount(String field, List<String> sortedTableNames, Set<String> fields, RangeSet<Long> ranges) throws IOException, StorageException {
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();
    for (String tableName : sortedTableNames) {
      try (Scanner<Long, Scanner<String, Object>> scanner = scan(tableName, fields, ranges)) {
        buffer.putRows(scanner);
      }
    }

    return buffer.count(field);
  }
}
